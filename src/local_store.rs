use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use parking_lot::RwLock;
use thiserror::Error;
use zerocopy::IntoBytes;

use crate::types::{Address, Context, Fragment, FragmentFlags, Hash, KeyType, Partition, hash_slice};

#[derive(Debug, Clone, Error)]
#[error("Address not found: {0}")]
pub struct AddressNotFound(pub Address);

impl From<Address> for AddressNotFound {
    fn from(addr: Address) -> Self {
        Self(addr)
    }
}

impl From<Hash> for AddressNotFound {
    fn from(hash: Hash) -> Self {
        Self(Address {
            hash,
            context: Context::ZERO,
        })
    }
}

#[derive(Debug, Clone, Error)]
pub enum StoreError {
    #[error("Address not found: {0}")]
    AddressNotFound(#[from] AddressNotFound),
    #[error("Payload not found")]
    PayloadNotFound,
    #[error("Slow down: {0}")]
    SlowDown(String),
    #[error("Not supported: {0}")]
    NotSupported(String),
    #[error("Internal store error: {0}")]
    Internal(String),
}

impl StoreError {
    pub fn is_address_not_found(&self) -> bool {
        matches!(self, Self::AddressNotFound(_))
    }

    pub fn not_supported(msg: impl Into<String>) -> Self {
        Self::NotSupported(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum StoreMatch {
    #[default]
    MatchNone = 0,
    MatchHash = 1,
    MatchPartition = 2,
    MatchFull = 3,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreMatchResult {
    pub match_made: StoreMatch,
    pub partition: Partition,
    pub context: Context,
    pub stored_local: bool,
    pub stored_durable: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreGetData {
    pub fragment: Fragment,
    pub payload: Option<Bytes>,
    pub match_made: StoreMatch,
    pub partition: Partition,
    pub context: Context,
}

impl StoreGetData {
    pub fn metadata(fragment: Fragment, match_made: StoreMatch, partition: Partition) -> Self {
        Self {
            fragment,
            payload: None,
            match_made,
            partition,
            context: Context::ZERO,
        }
    }

    pub fn into_payload(self) -> Result<(Fragment, Bytes), StoreError> {
        match self.payload {
            Some(payload) => Ok((self.fragment, payload)),
            None => Err(StoreError::PayloadNotFound),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreObliterateStats {
    pub obliterated_fragments: u64,
    pub obliterated_bytes: u64,
    pub obliterated_references: u64,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct CopyBehavior {
    pub durable: bool,
    pub do_not_replicate: bool,
}

pub struct CallerBuffer {
    buf: Vec<u8>,
}

impl CallerBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PayloadRead {
    Written { len: usize },
}

pub fn validate_buffer_capacity(
    fragment: &Fragment,
    dst: &mut CallerBuffer,
) -> Result<(), StoreError> {
    let required = fragment.size_payload as usize;
    if dst.buf.capacity() < required {
        dst.buf.reserve(required - dst.buf.len());
    }
    Ok(())
}

pub fn copy_into_caller_buffer(
    fragment: Fragment,
    payload: &[u8],
    dst: &mut CallerBuffer,
) -> Result<(Fragment, PayloadRead), StoreError> {
    validate_buffer_capacity(&fragment, dst)?;
    dst.buf.clear();
    dst.buf.extend_from_slice(payload);
    Ok((fragment, PayloadRead::Written { len: payload.len() }))
}

#[async_trait]
pub trait ImmutableStore: Send + Sync + 'static {
    fn is_local(&self) -> bool {
        true
    }

    fn isolates_partitions(&self) -> bool {
        true
    }

    fn read_scope(&self) -> StoreMatch {
        StoreMatch::MatchPartition
    }

    fn query_scope(&self) -> StoreMatch {
        StoreMatch::MatchPartition
    }

    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError>;

    async fn get_into(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        dst: &mut CallerBuffer,
    ) -> Result<(Fragment, PayloadRead), StoreError> {
        let (fragment, payload) = self.get(partition, address).await?.into_payload()?;
        copy_into_caller_buffer(fragment, &payload, dst)
    }

    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        let mut data = self.get(partition, address).await?;
        data.payload = None;
        Ok(data)
    }

    async fn is_available(self: Arc<Self>, _timeout: Duration) -> bool {
        true
    }

    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError>;

    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        fragment: Fragment,
        data: Option<Bytes>,
        no_replication: bool,
    ) -> Result<(), StoreError>;

    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        target_partition: Partition,
        target_context: Context,
        behavior: CopyBehavior,
    ) -> Result<(), StoreError>;

    async fn obliterate(
        self: Arc<Self>,
        _partition: Partition,
        _addresses: &[Address],
    ) -> Result<StoreObliterateStats, StoreError> {
        Ok(StoreObliterateStats::default())
    }

    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        Ok(())
    }
}

impl Debug for dyn ImmutableStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ImmutableStore")
    }
}

pub type KeyValueStream = Pin<Box<dyn Stream<Item = Result<(Hash, Hash), StoreError>> + Send>>;

#[async_trait]
pub trait MutableStore: Send + Sync + 'static {
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError>;

    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError>;

    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError>;

    async fn list(
        self: Arc<Self>,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError>;

    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        Ok(())
    }
}

impl Debug for dyn MutableStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MutableStore")
    }
}

// ============================================================================
// Disk-Backed `LocalImmutableStore`
// ============================================================================

struct ImmutableMemoryState {
    /// Content hash -> `(Fragment, Option<Bytes>)`.
    blobs: HashMap<Hash, (Fragment, Option<Bytes>)>,
    /// Full `(partition, hash, context)` associations.
    associations: HashSet<(Partition, Hash, Context)>,
    /// `(partition, hash)` -> first seen `Context` in that partition.
    partition_hashes: HashMap<(Partition, Hash), Context>,
}

pub struct LocalImmutableStore {
    root_dir: PathBuf,
    blobs_dir: PathBuf,
    assoc_log_path: PathBuf,
    state: RwLock<ImmutableMemoryState>,
}

impl LocalImmutableStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        let root_dir = path.as_ref().to_path_buf();
        let blobs_dir = root_dir.join("blobs");
        let assoc_log_path = root_dir.join("assoc.log");
        fs::create_dir_all(&blobs_dir)?;

        let mut blobs = HashMap::new();
        if let Ok(entries) = fs::read_dir(&blobs_dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) != Some("bin") {
                    continue;
                }
                let Some(stem) = p.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let Ok(hash) = stem.parse::<Hash>() else {
                    continue;
                };
                if let Ok(raw) = fs::read(&p)
                    && raw.len() >= 16
                {
                    let fragment = Fragment::from(&raw[0..16]);
                    let payload = if raw.len() > 16 {
                        Some(Bytes::copy_from_slice(&raw[16..]))
                    } else {
                        None
                    };
                    blobs.insert(hash, (fragment, payload));
                }
            }
        }

        let mut associations = HashSet::new();
        let mut partition_hashes = HashMap::new();
        if let Ok(assoc_bytes) = fs::read(&assoc_log_path) {
            for chunk in assoc_bytes.chunks_exact(64) {
                let part = Partition::from(&chunk[0..16]);
                let hash = Hash::from(&chunk[16..48]);
                let ctx = Context::from(&chunk[48..64]);
                associations.insert((part, hash, ctx));
                partition_hashes.entry((part, hash)).or_insert(ctx);
            }
        }

        Ok(Arc::new(Self {
            root_dir,
            blobs_dir,
            assoc_log_path,
            state: RwLock::new(ImmutableMemoryState {
                blobs,
                associations,
                partition_hashes,
            }),
        }))
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    fn persist_blob(&self, hash: Hash, fragment: Fragment, payload: Option<&Bytes>) {
        let file_path = self.blobs_dir.join(format!("{hash}.bin"));
        if file_path.exists() {
            return;
        }
        let mut buf = Vec::with_capacity(16 + payload.map_or(0, Bytes::len));
        buf.extend_from_slice(fragment.as_bytes());
        if let Some(p) = payload {
            buf.extend_from_slice(p);
        }
        let _ = fs::write(file_path, buf);
    }

    fn persist_association(&self, partition: Partition, hash: Hash, context: Context) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.assoc_log_path)
        {
            let mut record = [0u8; 64];
            record[0..16].copy_from_slice(partition.as_bytes());
            record[16..48].copy_from_slice(hash.as_bytes());
            record[48..64].copy_from_slice(context.as_bytes());
            let _ = file.write_all(&record);
        }
    }

    /// Helper to store an uncompressed structural or metadata blob directly and return its `Address`.
    pub fn put_uncompressed_blob(&self, partition: Partition, data: Bytes) -> Address {
        let hash = hash_slice(&data);
        let address = Address {
            hash,
            context: Context::ZERO,
        };
        let fragment = Fragment {
            flags: FragmentFlags::PayloadStored.bits() | FragmentFlags::PayloadStoredDurable.bits(),
            size_payload: data.len() as u32,
            size_content: data.len() as u64,
        };
        self.persist_blob(hash, fragment, Some(&data));
        let mut guard = self.state.write();
        guard.blobs.insert(hash, (fragment, Some(data)));
        if guard
            .associations
            .insert((partition, hash, address.context))
        {
            guard
                .partition_hashes
                .entry((partition, hash))
                .or_insert(address.context);
            self.persist_association(partition, hash, address.context);
        }
        address
    }
}

#[async_trait]
impl ImmutableStore for LocalImmutableStore {
    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        let guard = self.state.read();
        let Some((mut fragment, payload)) = guard.blobs.get(&address.hash).cloned() else {
            return Err(StoreError::AddressNotFound(AddressNotFound(address)));
        };

        let (match_made, resolved_ctx) =
            if guard
                .associations
                .contains(&(partition, address.hash, address.context))
            {
                (StoreMatch::MatchFull, address.context)
            } else if let Some(&ctx) = guard.partition_hashes.get(&(partition, address.hash)) {
                (StoreMatch::MatchPartition, ctx)
            } else {
                // Local store also allows reading by hash if partition is zero or matching blob exists
                (StoreMatch::MatchHash, address.context)
            };

        if payload.is_some() {
            fragment.flags |=
                FragmentFlags::PayloadStored.bits() | FragmentFlags::PayloadStoredDurable.bits();
        }

        Ok(StoreGetData {
            fragment,
            payload,
            match_made,
            partition,
            context: resolved_ctx,
        })
    }

    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        let guard = self.state.read();
        let Some((mut fragment, ref payload)) = guard.blobs.get(&address.hash).cloned() else {
            return Ok(StoreGetData {
                fragment: Fragment::default(),
                payload: None,
                match_made: StoreMatch::MatchNone,
                partition: Partition::ZERO,
                context: Context::ZERO,
            });
        };

        let (match_made, resolved_ctx) =
            if guard
                .associations
                .contains(&(partition, address.hash, address.context))
            {
                (StoreMatch::MatchFull, address.context)
            } else if let Some(&ctx) = guard.partition_hashes.get(&(partition, address.hash)) {
                (StoreMatch::MatchPartition, ctx)
            } else {
                return Ok(StoreGetData {
                    fragment: Fragment::default(),
                    payload: None,
                    match_made: StoreMatch::MatchNone,
                    partition: Partition::ZERO,
                    context: Context::ZERO,
                });
            };

        if payload.is_some() {
            fragment.flags |=
                FragmentFlags::PayloadStored.bits() | FragmentFlags::PayloadStoredDurable.bits();
        }

        Ok(StoreGetData {
            fragment,
            payload: None,
            match_made,
            partition,
            context: resolved_ctx,
        })
    }

    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        let guard = self.state.read();
        for (addr, out) in addresses.iter().zip(results.iter_mut()) {
            let Some((_frag, payload)) = guard.blobs.get(&addr.hash) else {
                *out = StoreMatchResult::default();
                continue;
            };
            let has_payload = payload.is_some();
            if guard
                .associations
                .contains(&(partition, addr.hash, addr.context))
            {
                *out = StoreMatchResult {
                    match_made: StoreMatch::MatchFull,
                    partition,
                    context: addr.context,
                    stored_local: has_payload,
                    stored_durable: has_payload,
                };
            } else if let Some(&ctx) = guard.partition_hashes.get(&(partition, addr.hash)) {
                *out = StoreMatchResult {
                    match_made: StoreMatch::MatchPartition,
                    partition,
                    context: ctx,
                    stored_local: has_payload,
                    stored_durable: has_payload,
                };
            } else {
                *out = StoreMatchResult::default();
            }
        }
        Ok(())
    }

    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        mut fragment: Fragment,
        data: Option<Bytes>,
        _no_replication: bool,
    ) -> Result<(), StoreError> {
        let has_payload = data.is_some();
        if has_payload {
            fragment.flags |=
                FragmentFlags::PayloadStored.bits() | FragmentFlags::PayloadStoredDurable.bits();
        }
        self.persist_blob(address.hash, fragment, data.as_ref());

        let mut guard = self.state.write();
        match guard.blobs.get_mut(&address.hash) {
            Some((existing_frag, existing_payload)) => {
                if existing_payload.is_none() && data.is_some() {
                    *existing_frag = fragment;
                    *existing_payload = data;
                }
            }
            None => {
                guard.blobs.insert(address.hash, (fragment, data));
            }
        }

        if guard
            .associations
            .insert((partition, address.hash, address.context))
        {
            guard
                .partition_hashes
                .entry((partition, address.hash))
                .or_insert(address.context);
            self.persist_association(partition, address.hash, address.context);
        }

        Ok(())
    }

    async fn copy(
        self: Arc<Self>,
        _source_partition: Partition,
        source_address: Address,
        target_partition: Partition,
        target_context: Context,
        _behavior: CopyBehavior,
    ) -> Result<(), StoreError> {
        let mut guard = self.state.write();
        if !guard.blobs.contains_key(&source_address.hash) {
            return Err(StoreError::AddressNotFound(AddressNotFound(source_address)));
        }
        if guard
            .associations
            .insert((target_partition, source_address.hash, target_context))
        {
            guard
                .partition_hashes
                .entry((target_partition, source_address.hash))
                .or_insert(target_context);
            self.persist_association(target_partition, source_address.hash, target_context);
        }
        Ok(())
    }
}

// ============================================================================
// Disk-Backed `LocalMutableStore`
// ============================================================================

pub struct LocalMutableStore {
    file_path: PathBuf,
    /// `(partition, key, key_type)` -> `value`
    entries: RwLock<HashMap<(Partition, Hash, KeyType), Hash>>,
}

impl LocalMutableStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let file_path = dir.join("mutable.bin");

        let mut entries = HashMap::new();
        if let Ok(raw) = fs::read(&file_path) {
            // Each record is: 16B partition + 32B key + 1B key_type + 32B value = 81 bytes
            for chunk in raw.chunks_exact(81) {
                let part = Partition::from(&chunk[0..16]);
                let key = Hash::from(&chunk[16..48]);
                let Ok(key_type) = KeyType::try_from(chunk[48]) else {
                    continue;
                };
                let val = Hash::from(&chunk[49..81]);
                if !val.is_zero() {
                    entries.insert((part, key, key_type), val);
                }
            }
        }

        Ok(Arc::new(Self {
            file_path,
            entries: RwLock::new(entries),
        }))
    }

    fn save_locked(&self, entries: &HashMap<(Partition, Hash, KeyType), Hash>) {
        let mut buf = Vec::with_capacity(entries.len() * 81);
        for (&(part, key, key_type), &val) in entries {
            buf.extend_from_slice(part.as_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.push(key_type as u8);
            buf.extend_from_slice(val.as_bytes());
        }
        let _ = fs::write(&self.file_path, buf);
    }
}

#[async_trait]
impl MutableStore for LocalMutableStore {
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        let guard = self.entries.read();
        guard
            .get(&(partition, key, key_type))
            .copied()
            .ok_or_else(|| StoreError::AddressNotFound(AddressNotFound::from(key)))
    }

    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError> {
        let mut guard = self.entries.write();
        if value.is_zero() {
            guard.remove(&(partition, key, key_type));
        } else {
            guard.insert((partition, key, key_type), value);
        }
        self.save_locked(&guard);
        Ok(())
    }

    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        let mut guard = self.entries.write();
        let current = guard
            .get(&(partition, key, key_type))
            .copied()
            .unwrap_or(Hash::ZERO);
        if current == expected {
            if value.is_zero() {
                guard.remove(&(partition, key, key_type));
            } else {
                guard.insert((partition, key, key_type), value);
            }
            self.save_locked(&guard);
        }
        Ok(current)
    }

    async fn list(
        self: Arc<Self>,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        if key_type == KeyType::Untyped {
            return Ok(Box::pin(futures::stream::empty()));
        }
        let guard = self.entries.read();
        let items: Vec<Result<(Hash, Hash), StoreError>> = guard
            .iter()
            .filter(|&(&(p, _, kt), _)| kt == key_type && (partition.is_zero() || p == partition))
            .map(|(&(_, k, _), &v)| Ok((k, v)))
            .collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }
}
