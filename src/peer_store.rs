use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashSet;
use iroh::EndpointAddr;
use serde::Deserialize;
use tracing::{debug, warn};
use zerocopy::IntoBytes;

use crate::local_store::{
    AddressNotFound, CallerBuffer, CopyBehavior, ImmutableStore, PayloadRead, StoreError,
    StoreGetData, StoreMatch, StoreMatchResult, StoreObliterateStats,
};
use crate::p2p_node::BitloreIrohNode;
use crate::store::is_structural_address;
use crate::tracker::{
    AnnounceRequest, BitloreTrackerClient, FileOwnership, GetStructuralRequest, HeartbeatRequest,
    PutStructuralRequest, QueryStructuralRequest, ResolveRequest, StructuralAddressProto,
};
use crate::types::{Address, Context, Fragment, FragmentFlags, Partition};

pub const PEER_PLUGIN_NAME: &str = "bitlore_peer";

#[derive(Debug, Clone, Deserialize)]
pub struct BitlorePeerStoreConfig {
    /// Local directory where this peer stores its fragments.
    #[serde(default)]
    pub path: String,
    /// gRPC address of `bitlore-server`'s P2P tracker (e.g. `"http://127.0.0.1:42340"`).
    #[serde(default = "default_tracker_url")]
    pub tracker_url: String,
    /// Whether to enable N0 public relay & DNS discovery in `iroh` (false for fast LAN/local mode).
    #[serde(default)]
    pub use_n0_relay: bool,
    #[serde(default = "default_flush_delay_seconds")]
    pub flush_delay_seconds: u64,
    #[serde(default)]
    pub max_capacity: Option<usize>,
    #[serde(default)]
    pub max_size: Option<usize>,
}

impl Default for BitlorePeerStoreConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            tracker_url: default_tracker_url(),
            use_n0_relay: false,
            flush_delay_seconds: default_flush_delay_seconds(),
            max_capacity: None,
            max_size: None,
        }
    }
}

fn default_tracker_url() -> String {
    "http://127.0.0.1:42340".to_string()
}

fn default_flush_delay_seconds() -> u64 {
    1
}

/// Client-side P2P `ImmutableStore` that bridges vanilla `lore` CLI commands with:
/// - `bitlore-server` (for structural Merkle tree fragments `context == 0` and P2P tracker discovery)
/// - Teammates' `bitlore-peer` nodes via `iroh` QUIC (for file content payloads `context != 0`)
pub struct BitlorePeerStore {
    local_store: Arc<dyn ImmutableStore>,
    iroh_node: Arc<BitloreIrohNode>,
    tracker: BitloreTrackerClient,
    /// Deduplicates `(repository_id, file_id)` announcements to the tracker per session.
    announced_files: DashSet<([u8; 16], [u8; 16])>,
    persistence_file: Option<PathBuf>,
}

impl BitlorePeerStore {
    pub fn new(
        local_store: Arc<dyn ImmutableStore>,
        iroh_node: Arc<BitloreIrohNode>,
        tracker: BitloreTrackerClient,
    ) -> Arc<Self> {
        Self::new_with_persistence_dir(local_store, iroh_node, tracker, None)
    }

    pub fn new_with_persistence_dir(
        local_store: Arc<dyn ImmutableStore>,
        iroh_node: Arc<BitloreIrohNode>,
        tracker: BitloreTrackerClient,
        persistence_dir: Option<PathBuf>,
    ) -> Arc<Self> {
        let announced_files = DashSet::new();
        let persistence_file = persistence_dir.map(|d| d.join("bitlore_announced_files.json"));

        if let Some(ref path) = persistence_file
            && let Ok(raw) = std::fs::read_to_string(path)
            && let Ok(entries) = serde_json::from_str::<Vec<(String, String)>>(&raw)
        {
            for (repo_hex, file_hex) in entries {
                if let (Ok(r_vec), Ok(f_vec)) = (hex::decode(repo_hex), hex::decode(file_hex))
                    && let (Ok(r), Ok(f)) = (r_vec.try_into(), f_vec.try_into())
                {
                    announced_files.insert((r, f));
                }
            }
        }

        let store = Arc::new(Self {
            local_store,
            iroh_node,
            tracker,
            announced_files,
            persistence_file,
        });

        // Spawn initial registration & periodic heartbeat with `bitlore-server`'s P2P tracker
        let heartbeat_store = store.clone();
        crate::spawn_bg(async move {
            let initial_files: Vec<FileOwnership> = heartbeat_store
                .announced_files
                .iter()
                .map(|entry| {
                    let (repo, file) = *entry.key();
                    FileOwnership {
                        repository_id: repo.to_vec(),
                        file_id: file.to_vec(),
                    }
                })
                .collect();

            let _ = heartbeat_store
                .tracker
                .announce(AnnounceRequest {
                    peer_id: heartbeat_store.iroh_node.peer_id(),
                    endpoint_addr_json: heartbeat_store.iroh_node.endpoint_addr_json().to_string(),
                    files: initial_files,
                })
                .await;

            let mut interval = tokio::time::interval(Duration::from_secs(20));
            loop {
                interval.tick().await;
                if let Ok(resp) = heartbeat_store
                    .tracker
                    .heartbeat(HeartbeatRequest {
                        peer_id: heartbeat_store.iroh_node.peer_id(),
                        endpoint_addr_json: heartbeat_store
                            .iroh_node
                            .endpoint_addr_json()
                            .to_string(),
                    })
                    .await
                    && !resp.known
                {
                    let files: Vec<FileOwnership> = heartbeat_store
                        .announced_files
                        .iter()
                        .map(|entry| {
                            let (repo, file) = *entry.key();
                            FileOwnership {
                                repository_id: repo.to_vec(),
                                file_id: file.to_vec(),
                            }
                        })
                        .collect();
                    let _ = heartbeat_store
                        .tracker
                        .announce(AnnounceRequest {
                            peer_id: heartbeat_store.iroh_node.peer_id(),
                            endpoint_addr_json: heartbeat_store
                                .iroh_node
                                .endpoint_addr_json()
                                .to_string(),
                            files,
                        })
                        .await;
                }
            }
        });

        store
    }

    pub fn iroh_node(&self) -> &Arc<BitloreIrohNode> {
        &self.iroh_node
    }

    fn persist_announced_files(&self) {
        if let Some(ref path) = self.persistence_file {
            let entries: Vec<(String, String)> = self
                .announced_files
                .iter()
                .map(|entry| {
                    let (r, f) = *entry.key();
                    (hex::encode(r), hex::encode(f))
                })
                .collect();
            if let Ok(json) = serde_json::to_string(&entries) {
                let _ = std::fs::write(path, json);
            }
        }
    }

    async fn announce_files_if_needed(&self, partition: Partition, contexts: &[Context]) {
        let repo_bytes: [u8; 16] = (*partition.as_bytes()).try_into().unwrap();
        let mut new_ownerships = Vec::new();

        for ctx in contexts {
            if ctx.is_zero() {
                continue;
            }
            let file_bytes: [u8; 16] = (*ctx.as_bytes()).try_into().unwrap();
            if self.announced_files.insert((repo_bytes, file_bytes)) {
                new_ownerships.push(FileOwnership {
                    repository_id: repo_bytes.to_vec(),
                    file_id: file_bytes.to_vec(),
                });
            }
        }

        if !new_ownerships.is_empty() {
            self.persist_announced_files();
            if let Err(e) = self
                .tracker
                .announce(AnnounceRequest {
                    peer_id: self.iroh_node.peer_id(),
                    endpoint_addr_json: self.iroh_node.endpoint_addr_json().to_string(),
                    files: new_ownerships,
                })
                .await
            {
                warn!(error = %e, "Failed to announce file ownership to Bitlore P2P tracker");
            }
        }
    }

    async fn fetch_from_p2p_swarm(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<(Fragment, Bytes), StoreError> {
        let resp = self
            .tracker
            .resolve(ResolveRequest {
                repository_id: partition.as_bytes().to_vec(),
                file_id: address.context.as_bytes().to_vec(),
                requester_peer_id: self.iroh_node.peer_id(),
            })
            .await
            .map_err(|e| {
                warn!(%partition, %address, error = %e, "Bitlore P2P Tracker resolve failed");
                StoreError::from(AddressNotFound::from(address))
            })?;

        if resp.peers.is_empty() {
            warn!(
                %partition,
                %address,
                "Bitlore P2P: no online peers found holding file fragment"
            );
            return Err(StoreError::from(AddressNotFound::from(address)));
        }

        for peer_info in &resp.peers {
            let peer_addr: EndpointAddr = match serde_json::from_str(&peer_info.endpoint_addr_json)
            {
                Ok(addr) => addr,
                Err(e) => {
                    warn!(peer_id = %peer_info.peer_id, error = %e, "Invalid peer EndpointAddr JSON");
                    continue;
                }
            };

            match self
                .iroh_node
                .fetch_chunk_from_peer(&peer_addr, partition, address)
                .await
            {
                Ok((fragment, payload)) => {
                    // Cache in our local store so subsequent reads hit disk and we can seed to other peers
                    let _ = self
                        .local_store
                        .clone()
                        .put(partition, address, fragment, Some(payload.clone()), true)
                        .await;
                    let _ = self.local_store.clone().flush(false).await;

                    // Announce to the tracker that we now hold this `file_id` too!
                    self.announce_files_if_needed(partition, &[address.context])
                        .await;

                    return Ok((fragment, payload));
                }
                Err(e) => {
                    warn!(
                        peer_id = %peer_info.peer_id,
                        %partition,
                        %address,
                        error = %e,
                        "Failed to fetch chunk from candidate peer, trying next peer"
                    );
                }
            }
        }

        Err(StoreError::from(AddressNotFound::from(address)))
    }
}

#[async_trait]
impl ImmutableStore for BitlorePeerStore {
    fn is_local(&self) -> bool {
        self.local_store.is_local()
    }

    fn isolates_partitions(&self) -> bool {
        self.local_store.isolates_partitions()
    }

    fn read_scope(&self) -> StoreMatch {
        self.local_store.read_scope()
    }

    fn query_scope(&self) -> StoreMatch {
        self.local_store.query_scope()
    }

    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        // 1. Check local store first
        if let Ok(data) = self.local_store.clone().get(partition, address).await
            && data.payload.is_some()
        {
            return Ok(data);
        }

        // 2. If structural fragment (`context == 0`), fetch from `bitlore-server`
        if is_structural_address(&address) {
            let resp = self
                .tracker
                .get_structural(GetStructuralRequest {
                    repository_id: partition.as_bytes().to_vec(),
                    hash: address.hash.as_bytes().to_vec(),
                    context: address.context.as_bytes().to_vec(),
                })
                .await
                .map_err(|_| StoreError::from(AddressNotFound::from(address)))?;

            if !resp.found {
                return Err(StoreError::from(AddressNotFound::from(address)));
            }

            let fragment = Fragment {
                size_payload: resp.size_payload,
                size_content: resp.size_content,
                flags: resp.flags | FragmentFlags::PayloadStoredDurable.bits(),
            };
            let _ = self
                .local_store
                .clone()
                .put(
                    partition,
                    address,
                    fragment,
                    Some(resp.payload.clone()),
                    true,
                )
                .await;

            return Ok(StoreGetData {
                fragment,
                payload: Some(resp.payload),
                match_made: StoreMatch::MatchFull,
                partition,
                context: address.context,
            });
        }

        // 3. File content fragment (`context != 0`): fetch on-demand Peer-to-Peer via `iroh`!
        let (fragment, payload) = self.fetch_from_p2p_swarm(partition, address).await?;
        Ok(StoreGetData {
            fragment,
            payload: Some(payload),
            match_made: StoreMatch::MatchFull,
            partition,
            context: address.context,
        })
    }

    async fn get_into(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        dst: &mut CallerBuffer,
    ) -> Result<(Fragment, PayloadRead), StoreError> {
        if let Ok(res) = self
            .local_store
            .clone()
            .get_into(partition, address, dst)
            .await
        {
            return Ok(res);
        }

        // Fetch from bitlore-server (for structural) or P2P swarm (for file chunks), which caches
        // the verified fragment in `self.local_store`, then read into `dst`.
        let _data = self.clone().get(partition, address).await?;
        self.local_store
            .clone()
            .get_into(partition, address, dst)
            .await
    }

    async fn is_available(self: Arc<Self>, timeout: Duration) -> bool {
        self.local_store.clone().is_available(timeout).await
    }

    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        // First query our local store so we know which file chunks we actually hold on disk.
        // If the user committed with `--local`, missing file chunks return `MatchNone` here so
        // `lore push` copies them over localhost into this `bitlore-peer`'s local store!
        self.local_store
            .clone()
            .query(partition, addresses, results)
            .await?;

        let mut structural_indices = Vec::new();
        let mut structural_protos = Vec::new();
        let mut file_contexts = Vec::new();

        for (i, addr) in addresses.iter().enumerate() {
            if is_structural_address(addr) {
                structural_indices.push(i);
                structural_protos.push(StructuralAddressProto {
                    hash: addr.hash.as_bytes().to_vec(),
                    context: addr.context.as_bytes().to_vec(),
                });
            } else if results[i].match_made == StoreMatch::MatchFull {
                file_contexts.push(addr.context);
                results[i].stored_local = true;
                results[i].stored_durable = true;
            }
        }

        if !file_contexts.is_empty() {
            self.announce_files_if_needed(partition, &file_contexts)
                .await;
        }

        if !structural_protos.is_empty() {
            let resp = self
                .tracker
                .query_structural(QueryStructuralRequest {
                    repository_id: partition.as_bytes().to_vec(),
                    addresses: structural_protos,
                })
                .await
                .map_err(|_| {
                    StoreError::not_supported(
                        "failed to query structural fragments on bitlore-server",
                    )
                })?;

            for (idx, is_present) in structural_indices.into_iter().zip(resp.present) {
                results[idx] = if is_present {
                    StoreMatchResult {
                        match_made: StoreMatch::MatchFull,
                        partition,
                        context: addresses[idx].context,
                        stored_local: true,
                        stored_durable: true,
                    }
                } else {
                    StoreMatchResult {
                        match_made: StoreMatch::MatchNone,
                        partition,
                        context: addresses[idx].context,
                        stored_local: false,
                        stored_durable: false,
                    }
                };
            }
        }

        let _ = self.local_store.clone().flush(false).await;
        Ok(())
    }

    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        if let Ok(meta) = self
            .local_store
            .clone()
            .get_metadata(partition, address)
            .await
            && meta.match_made != StoreMatch::MatchNone
        {
            return Ok(meta);
        }

        if is_structural_address(&address) {
            let data = self.get(partition, address).await?;
            return Ok(StoreGetData::metadata(
                data.fragment,
                StoreMatch::MatchFull,
                partition,
            ));
        }

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
        // 1. Always store locally so this peer has the data to serve to teammates over `iroh`
        self.local_store
            .clone()
            .put(partition, address, fragment, payload.clone(), force)
            .await?;

        // 2. If structural (`context == 0`), forward to `bitlore-server` and flush local store index
        if is_structural_address(&address) {
            if let Some(bytes) = payload {
                self.tracker
                    .put_structural(PutStructuralRequest {
                        repository_id: partition.as_bytes().to_vec(),
                        hash: address.hash.as_bytes().to_vec(),
                        context: address.context.as_bytes().to_vec(),
                        size_payload: fragment.size_payload,
                        size_content: fragment.size_content,
                        flags: fragment.flags,
                        payload: bytes,
                    })
                    .await
                    .map_err(|e| {
                        warn!(%partition, %address, error = %e, "Failed to forward structural fragment to bitlore-server");
                        StoreError::not_supported(format!("failed to forward structural fragment: {e}"))
                    })?;
            }
            let _ = self.local_store.clone().flush(false).await;
            return Ok(());
        }

        // 3. File content fragment (`context != 0`): announce `(repository_id, file_id)` to tracker!
        //    Zero file bytes are sent to `bitlore-server`.
        let _ = self.local_store.clone().flush(false).await;
        self.announce_files_if_needed(partition, &[address.context])
            .await;
        debug!(
            %partition,
            %address,
            size_payload = fragment.size_payload,
            "BitlorePeerStore: stored file chunk locally and announced file_id to P2P tracker"
        );
        Ok(())
    }

    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
    ) -> Result<StoreObliterateStats, StoreError> {
        self.local_store
            .clone()
            .obliterate(partition, addresses)
            .await
    }

    async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
        self.local_store.clone().flush(sync_data).await
    }

    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        behavior: CopyBehavior,
    ) -> Result<(), StoreError> {
        self.local_store
            .clone()
            .copy(
                source_partition,
                source_address,
                destination_partition,
                destination_context,
                behavior,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_store::LocalImmutableStore;
    use crate::store::BitloreMetadataStore;
    use crate::tracker::spawn_tracker_grpc_server;
    use crate::types::hash_slice;
    use std::sync::atomic::Ordering;

    async fn create_temp_local_store(dir: &std::path::Path) -> Arc<dyn ImmutableStore> {
        LocalImmutableStore::open(dir).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_p2p_tracker_and_iroh_chunk_swarm_between_two_peers() {
        // 1. Start `bitlore-server`'s metadata store + P2P Tracker gRPC service on a free port
        let server_dir = tempfile::tempdir().unwrap();
        let server_inner = create_temp_local_store(server_dir.path()).await;
        let server_meta_store = Arc::new(BitloreMetadataStore::new(server_inner, false));

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let tracker_addr = listener.local_addr().unwrap();
        drop(listener);

        let tracker_state = spawn_tracker_grpc_server(
            tracker_addr,
            server_meta_store.clone(),
            Duration::from_secs(120),
        );
        // Give tonic server a brief moment to bind
        tokio::time::sleep(Duration::from_millis(100)).await;

        let tracker_url = format!("http://{tracker_addr}");

        // 2. Create Peer A (Seeder / Pusher) and Peer B (Leecher / Cloner with empty local store)
        let peer_a_dir = tempfile::tempdir().unwrap();
        let peer_a_local = create_temp_local_store(peer_a_dir.path()).await;
        let peer_a_iroh = BitloreIrohNode::bind(peer_a_local.clone(), false)
            .await
            .unwrap();
        let peer_a_store = BitlorePeerStore::new(
            peer_a_local.clone(),
            peer_a_iroh.clone(),
            BitloreTrackerClient::new(&tracker_url),
        );

        let peer_b_dir = tempfile::tempdir().unwrap();
        let peer_b_local = create_temp_local_store(peer_b_dir.path()).await;
        let peer_b_iroh = BitloreIrohNode::bind(peer_b_local.clone(), false)
            .await
            .unwrap();
        let peer_b_store = BitlorePeerStore::new(
            peer_b_local.clone(),
            peer_b_iroh.clone(),
            BitloreTrackerClient::new(&tracker_url),
        );

        let repo_partition = Partition::from(&[0x77u8; 16]);
        let file_id_context = Context::from(&[0x42u8; 16]);

        // 3. Peer A commits & pushes:
        //    - A structural Merkle tree fragment (`context == 0`)
        //    - A file content fragment (`context == file_id_context`)
        let tree_bytes = Bytes::from_static(b"merkle-tree-node-block-structural-bytes");
        let tree_addr = Address {
            hash: hash_slice(&tree_bytes),
            context: Context::default(),
        };
        let tree_frag = Fragment {
            size_payload: tree_bytes.len() as u32,
            size_content: tree_bytes.len() as u64,
            flags: FragmentFlags::PayloadStoredDurable.bits(),
        };

        let file_bytes = Bytes::from(vec![0xABu8; 256 * 1024]); // 256 KiB binary asset chunk
        let file_addr = Address {
            hash: hash_slice(&file_bytes),
            context: file_id_context,
        };
        let file_frag = Fragment {
            size_payload: file_bytes.len() as u32,
            size_content: file_bytes.len() as u64,
            flags: FragmentFlags::PayloadStoredDurable.bits(),
        };

        peer_a_store
            .clone()
            .put(
                repo_partition,
                tree_addr,
                tree_frag,
                Some(tree_bytes.clone()),
                false,
            )
            .await
            .unwrap();

        peer_a_store
            .clone()
            .put(
                repo_partition,
                file_addr,
                file_frag,
                Some(file_bytes.clone()),
                false,
            )
            .await
            .unwrap();

        // Verify `bitlore-server` received the structural fragment (`context == 0`),
        // AND received ZERO bytes of the 256 KiB file content fragment!
        assert_eq!(
            server_meta_store
                .stats()
                .structural_puts
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            server_meta_store
                .stats()
                .structural_bytes_stored
                .load(Ordering::Relaxed),
            tree_bytes.len() as u64
        );
        assert!(
            server_meta_store
                .inner()
                .clone()
                .get(repo_partition, file_addr)
                .await
                .is_err(),
            "bitlore-server must NOT store file content fragments"
        );

        // Verify the P2P tracker recorded Peer A as an active seeder for (repo_partition, file_id_context)
        let resolve_req = ResolveRequest {
            repository_id: repo_partition.as_bytes().to_vec(),
            file_id: file_id_context.as_bytes().to_vec(),
            requester_peer_id: String::new(),
        };
        let resolved_peers = tracker_state.resolve(&resolve_req);
        assert_eq!(resolved_peers.len(), 1);
        assert_eq!(resolved_peers[0].peer_id, peer_a_iroh.peer_id());

        // 4. Now Peer B (empty local store!) clones/reads both the structural fragment and the
        //    256 KiB file content fragment on-demand!
        let fetched_tree = peer_b_store
            .clone()
            .get(repo_partition, tree_addr)
            .await
            .unwrap();
        assert_eq!(fetched_tree.payload.unwrap(), tree_bytes);

        let fetched_file = peer_b_store
            .clone()
            .get(repo_partition, file_addr)
            .await
            .unwrap();
        assert_eq!(fetched_file.payload.unwrap(), file_bytes);

        // Verify `iroh` P2P transfer stats on Peer A and Peer B
        assert_eq!(peer_a_iroh.stats().chunks_served.load(Ordering::Relaxed), 1);
        assert_eq!(
            peer_a_iroh.stats().bytes_served.load(Ordering::Relaxed),
            file_bytes.len() as u64
        );
        assert_eq!(
            peer_b_iroh.stats().chunks_fetched.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            peer_b_iroh.stats().bytes_fetched.load(Ordering::Relaxed),
            file_bytes.len() as u64
        );

        // Verify Peer B also cached the chunk locally AND announced itself to the tracker as a
        // new seeder in the swarm!
        let resolved_swarm = tracker_state.resolve(&resolve_req);
        assert_eq!(
            resolved_swarm.len(),
            2,
            "Both Peer A and Peer B should now be registered seeders in the swarm"
        );
    }
}
