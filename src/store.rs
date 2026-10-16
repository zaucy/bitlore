use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;
use tracing::{debug, info};

use crate::local_store::{
    AddressNotFound, CallerBuffer, CopyBehavior, ImmutableStore, PayloadRead, StoreError,
    StoreGetData, StoreMatch, StoreMatchResult, StoreObliterateStats,
};
use crate::types::{Address, Context, Fragment, FragmentFlags, Partition};

pub const PLUGIN_NAME: &str = "bitlore";

/// Configuration for the `bitlore` metadata-only immutable store.
#[derive(Debug, Clone, Deserialize)]
pub struct BitloreStoreConfig {
    /// Directory on disk where structural/metadata fragments (`context == 0`) are stored.
    /// When empty or omitted, defaults to `<temp_dir>/bitlore-server/immutable`.
    #[serde(default)]
    pub path: String,

    /// Background flush interval in seconds for the underlying local store.
    #[serde(default = "default_flush_delay_seconds")]
    pub flush_delay_seconds: u64,

    /// Optional maximum fragment count for the underlying local store.
    #[serde(default)]
    pub max_capacity: Option<usize>,

    /// Optional maximum byte size for the underlying local store.
    #[serde(default)]
    pub max_size: Option<usize>,

    /// When `false` (the default), `bitlore` operates in pure metadata-only mode:
    /// - Structural and metadata fragments (`address.context.is_zero()`) are stored and verified.
    /// - File content fragments (`!address.context.is_zero()`) are answered as `MatchFull` +
    ///   `PayloadStoredDurable` on `Query` and `get_metadata`, so vanilla `lore` clients upload
    ///   zero bytes of file payloads during `lore push` while `BranchPush` still verifies the
    ///   entire commit/tree structure.
    #[serde(default)]
    pub store_file_payloads: bool,

    /// Optional socket address (`host:port`) where `bitlore-server` hosts its unary gRPC P2P
    /// tracker service (`/bitlore.p2p.v1.P2pTracker/{Announce, Resolve, Heartbeat, ...}`).
    /// Set to empty string `""` to disable the P2P tracker listener.
    #[serde(default = "default_tracker_bind_addr")]
    pub tracker_bind_addr: String,

    /// Peer announcement time-to-live (TTL) in seconds for the P2P tracker.
    #[serde(default = "default_peer_ttl_seconds")]
    pub peer_ttl_seconds: u64,
}

impl Default for BitloreStoreConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            flush_delay_seconds: default_flush_delay_seconds(),
            max_capacity: None,
            max_size: None,
            store_file_payloads: false,
            tracker_bind_addr: default_tracker_bind_addr(),
            peer_ttl_seconds: default_peer_ttl_seconds(),
        }
    }
}

fn default_flush_delay_seconds() -> u64 {
    10
}

fn default_tracker_bind_addr() -> String {
    "0.0.0.0:42340".to_string()
}

fn default_peer_ttl_seconds() -> u64 {
    120
}

/// Live telemetry counters for `BitloreMetadataStore`.
#[derive(Debug, Default)]
pub struct BitloreStats {
    /// Number of structural/metadata fragments (`context == 0`) written to disk.
    pub structural_puts: AtomicU64,
    /// Total payload bytes of structural/metadata fragments written to disk.
    pub structural_bytes_stored: AtomicU64,
    /// Number of file content fragment queries (`context != 0`) intercepted and answered as
    /// `MatchFull` so the client skips uploading file bytes.
    pub file_queries_intercepted: AtomicU64,
    /// Number of file content fragment puts (`context != 0`) skipped.
    pub file_puts_skipped: AtomicU64,
}

/// Returns `true` if `address` identifies a Lore structural or metadata fragment
/// (`StateData`, `Tree`, `NodeBlock`, `NameTable`, `NodeFileMetadataBlock`, `LinkReference`,
/// or `Metadata` blob), all of which are addressed with `Context::default()` (`context.is_zero()`).
///
/// Returns `false` if `address` identifies a file content fragment (either a file's root
/// fragment or a child `FragmentReference` of a fragmented >1 MiB file), all of which carry
/// the file's non-zero UUIDv7 `file_id` in `address.context`.
#[inline]
pub fn is_structural_address(address: &Address) -> bool {
    address.context.is_zero()
}

/// Metadata-only `ImmutableStore` wrapper for the `bitlore` hosting platform.
///
/// Wraps a real backing `ImmutableStore` (such as `LocalImmutableStore`) and stores all
/// structural Merkle tree and commit metadata fragments (`address.context.is_zero()`), while
/// short-circuiting file content fragments (`!address.context.is_zero()`) so that an unmodified
/// vanilla `lore` client uploads 0 bytes of file payloads on `lore push`.
pub struct BitloreMetadataStore {
    inner: Arc<dyn ImmutableStore>,
    store_file_payloads: bool,
    stats: Arc<BitloreStats>,
}

impl BitloreMetadataStore {
    pub fn new(inner: Arc<dyn ImmutableStore>, store_file_payloads: bool) -> Self {
        Self {
            inner,
            store_file_payloads,
            stats: Arc::new(BitloreStats::default()),
        }
    }

    pub fn stats(&self) -> &Arc<BitloreStats> {
        &self.stats
    }

    pub fn inner(&self) -> &Arc<dyn ImmutableStore> {
        &self.inner
    }
}

#[async_trait]
impl ImmutableStore for BitloreMetadataStore {
    fn is_local(&self) -> bool {
        self.inner.is_local()
    }

    fn isolates_partitions(&self) -> bool {
        self.inner.isolates_partitions()
    }

    fn read_scope(&self) -> StoreMatch {
        self.inner.read_scope()
    }

    fn query_scope(&self) -> StoreMatch {
        self.inner.query_scope()
    }

    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        if is_structural_address(&address) || self.store_file_payloads {
            return self.inner.clone().get(partition, address).await;
        }

        // Check if the file fragment happens to be in the backing store anyway; otherwise
        // return AddressNotFound so `handle_get` maps it to `FragmentNotFound`
        // (avoiding client stale-session retries).
        match self.inner.clone().get(partition, address).await {
            Ok(data) => Ok(data),
            Err(_) => {
                debug!(
                    %partition,
                    %address,
                    "Client requested file content payload from metadata-only bitlore store; \
                     use a shared/P2P store (--shared-store) or external storage_url for file bytes"
                );
                Err(StoreError::from(AddressNotFound::from(address)))
            }
        }
    }

    async fn get_into(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        dst: &mut CallerBuffer,
    ) -> Result<(Fragment, PayloadRead), StoreError> {
        if is_structural_address(&address) || self.store_file_payloads {
            return self.inner.clone().get_into(partition, address, dst).await;
        }

        match self.inner.clone().get_into(partition, address, dst).await {
            Ok(res) => Ok(res),
            Err(_) => {
                debug!(
                    %partition,
                    %address,
                    "Client requested file content payload (get_into) from metadata-only bitlore store"
                );
                Err(StoreError::from(AddressNotFound::from(address)))
            }
        }
    }

    async fn is_available(self: Arc<Self>, timeout: Duration) -> bool {
        self.inner.clone().is_available(timeout).await
    }

    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        if self.store_file_payloads {
            return self
                .inner
                .clone()
                .query(partition, addresses, results)
                .await;
        }

        // Partition the incoming query batch into:
        // 1. Structural/metadata addresses (`context.is_zero()`) -> must be queried against `inner`
        //    so missing structural fragments are uploaded by the client and verified on BranchPush.
        // 2. File content addresses (`!context.is_zero()`) -> answered immediately with `MatchFull`
        //    and `stored_durable = true` so the vanilla client skips uploading file payloads.
        let mut structural_indices = Vec::new();
        let mut structural_addresses = Vec::new();
        let mut intercepted_files = 0u64;

        for (idx, addr) in addresses.iter().enumerate() {
            if is_structural_address(addr) {
                structural_indices.push(idx);
                structural_addresses.push(*addr);
            } else {
                results[idx] = StoreMatchResult {
                    match_made: StoreMatch::MatchFull,
                    partition,
                    context: addr.context,
                    stored_local: true,
                    stored_durable: true,
                };
                intercepted_files += 1;
            }
        }

        if intercepted_files > 0 {
            let total_intercepted = self
                .stats
                .file_queries_intercepted
                .fetch_add(intercepted_files, Ordering::Relaxed)
                + intercepted_files;
            info!(
                %partition,
                intercepted_files,
                structural_queries = structural_addresses.len(),
                total_intercepted,
                "Bitlore metadata-only query: intercepted file fragments (client will skip file upload)"
            );
        }

        if !structural_addresses.is_empty() {
            let mut structural_results =
                vec![StoreMatchResult::default(); structural_addresses.len()];
            self.inner
                .clone()
                .query(
                    partition,
                    structural_addresses.as_slice(),
                    &mut structural_results,
                )
                .await?;

            for (idx, res) in structural_indices.into_iter().zip(structural_results) {
                results[idx] = res;
            }
        }

        Ok(())
    }

    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        if is_structural_address(&address) || self.store_file_payloads {
            return self.inner.clone().get_metadata(partition, address).await;
        }

        // For file content fragments (`!address.context.is_zero()`), return a synthetic durable
        // unfragmented header so server-side `BranchPush` treats file fragments as already durably
        // stored without needing their payloads on disk.
        Ok(StoreGetData::metadata(
            Fragment {
                size_payload: 0,
                size_content: 0,
                flags: FragmentFlags::PayloadStoredDurable.bits(),
            },
            StoreMatch::MatchFull,
            partition,
        ))
    }

    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
        force: bool,
    ) -> Result<(), StoreError> {
        if is_structural_address(&address) || self.store_file_payloads {
            let payload_bytes = payload.as_ref().map_or(0, |b| b.len() as u64);
            self.inner
                .clone()
                .put(partition, address, fragment, payload, force)
                .await?;

            let count = self.stats.structural_puts.fetch_add(1, Ordering::Relaxed) + 1;
            let total_bytes = self
                .stats
                .structural_bytes_stored
                .fetch_add(payload_bytes, Ordering::Relaxed)
                + payload_bytes;
            debug!(
                %partition,
                %address,
                payload_bytes,
                total_structural_puts = count,
                total_structural_bytes = total_bytes,
                "Bitlore stored structural/metadata fragment"
            );
            return Ok(());
        }

        // If a client runs `lore commit` without `--local` (where the client attempts a direct
        // `Put` before running `Query`), returning `NotSupported` instructs `lore-storage`'s
        // `leader_body` (`write.rs`) that the payload is not stored on the metadata-only server
        // (`stored_durable = false`), which ensures the vanilla client retains the file payload
        // in its own local or P2P shared store (`if !stored_durable || cache_local`).
        // Subsequent `lore push` queries via `query()` receive `MatchFull` and skip uploading.
        let skipped = self.stats.file_puts_skipped.fetch_add(1, Ordering::Relaxed) + 1;
        info!(
            %partition,
            %address,
            size_payload = fragment.size_payload,
            total_file_puts_skipped = skipped,
            "Bitlore metadata-only mode: rejecting direct file content payload Put (client will retain payload locally)"
        );
        Err(StoreError::not_supported(
            "bitlore metadata-only store does not store file content payloads; use --local on commit or a shared/external store",
        ))
    }

    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
    ) -> Result<StoreObliterateStats, StoreError> {
        if self.store_file_payloads {
            return self.inner.clone().obliterate(partition, addresses).await;
        }
        let structural: Vec<Address> = addresses
            .iter()
            .copied()
            .filter(is_structural_address)
            .collect();
        if !structural.is_empty() {
            return self.inner.clone().obliterate(partition, &structural).await;
        }
        Ok(StoreObliterateStats::default())
    }

    async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
        self.inner.clone().flush(sync_data).await
    }

    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        behavior: CopyBehavior,
    ) -> Result<(), StoreError> {
        if (is_structural_address(&source_address) && destination_context.is_zero())
            || self.store_file_payloads
        {
            return self
                .inner
                .clone()
                .copy(
                    source_partition,
                    source_address,
                    destination_partition,
                    destination_context,
                    behavior,
                )
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_store::LocalImmutableStore;
    use crate::types::{Hash, hash_slice};

    #[tokio::test]
    async fn test_bitlore_store_separates_structural_and_file_fragments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let inner = LocalImmutableStore::open(temp_dir.path()).unwrap();

        let store = Arc::new(BitloreMetadataStore::new(inner, false));
        let partition = Partition::from(&[42u8; 16]);

        // 1. Structural address (context == 0) vs File address (context != 0)
        let payload_bytes = Bytes::from_static(b"tree-node-block-bytes");
        let structural_hash = hash_slice(&payload_bytes);
        let structural_addr = Address {
            hash: structural_hash,
            context: Context::default(),
        };

        let file_addr = Address {
            hash: Hash::from_u64(0xABCD),
            context: Context::from(&[9u8; 16]),
        };

        // 2. Query before put:
        //    - structural_addr must return MatchNone (so vanilla client uploads it)
        //    - file_addr must return MatchFull + stored_durable (so vanilla client skips upload)
        let mut results = [StoreMatchResult::default(); 2];
        store
            .clone()
            .query(partition, &[structural_addr, file_addr], &mut results)
            .await
            .unwrap();

        assert_eq!(results[0].match_made, StoreMatch::MatchNone);
        assert_eq!(results[1].match_made, StoreMatch::MatchFull);
        assert!(results[1].stored_durable);
        assert_eq!(
            store.stats().file_queries_intercepted.load(Ordering::Relaxed),
            1
        );

        // 3. Put structural fragment -> stored in inner store
        let fragment = Fragment {
            size_payload: payload_bytes.len() as u32,
            size_content: payload_bytes.len() as u64,
            flags: 0,
        };
        store
            .clone()
            .put(
                partition,
                structural_addr,
                fragment,
                Some(payload_bytes.clone()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(store.stats().structural_puts.load(Ordering::Relaxed), 1);
        assert_eq!(
            store.stats().structural_bytes_stored.load(Ordering::Relaxed),
            payload_bytes.len() as u64
        );

        // 4. Query again -> both structural_addr and file_addr return MatchFull!
        let mut results_after = [StoreMatchResult::default(); 2];
        store
            .clone()
            .query(partition, &[structural_addr, file_addr], &mut results_after)
            .await
            .unwrap();
        assert_eq!(results_after[0].match_made, StoreMatch::MatchFull);
        assert!(results_after[0].stored_durable);
        assert_eq!(results_after[1].match_made, StoreMatch::MatchFull);
        assert!(results_after[1].stored_durable);

        // 5. get_metadata on file_addr returns synthetic durable metadata (for BranchPush verify)
        let file_meta = store
            .clone()
            .get_metadata(partition, file_addr)
            .await
            .unwrap();
        assert_eq!(file_meta.match_made, StoreMatch::MatchFull);
        assert_ne!(
            file_meta.fragment.flags & FragmentFlags::PayloadStoredDurable.bits(),
            0
        );

        // 6. Direct put on file_addr returns NotSupported so clients retain payload locally
        let file_put_res = store
            .clone()
            .put(
                partition,
                file_addr,
                fragment,
                Some(Bytes::from_static(b"file-content")),
                false,
            )
            .await;
        assert!(file_put_res.is_err());
        assert_eq!(store.stats().file_puts_skipped.load(Ordering::Relaxed), 1);
    }
}
