use std::fmt::{self, Debug, Display};
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign};
use std::str::FromStr;

use anyhow::{Result, anyhow, bail};
use bitflags::bitflags;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub const FRAGMENT_SIZE_EXPECTED: usize = 64 * 1024;
pub const FRAGMENT_SIZE_THRESHOLD: usize = 256 * 1024;
pub const SALT_LORE: &[u8] = b"urc";

// ============================================================================
// Context / Partition / RepositoryId / BranchId (16 bytes)
// ============================================================================

#[repr(C)]
#[derive(
    Copy,
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    std::hash::Hash,
    IntoBytes,
    FromBytes,
    Immutable,
    KnownLayout,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct Context {
    pub data: [u8; 16],
}

pub type Partition = Context;
pub type RepositoryId = Context;
pub type BranchId = Context;

impl Context {
    pub const ZERO: Self = Self { data: [0u8; 16] };

    pub fn is_zero(&self) -> bool {
        self.data == [0u8; 16]
    }

    pub fn new_uuid_v7() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut h = blake3::Hasher::new();
        h.update(&ns.to_le_bytes());
        h.update(&seq.to_le_bytes());
        let digest = h.finalize();
        let mut bytes: [u8; 16] = digest.as_bytes()[0..16].try_into().unwrap();
        let ms = (ns / 1_000_000) as u64;
        bytes[0..6].copy_from_slice(&ms.to_be_bytes()[2..8]);
        bytes[6] = (bytes[6] & 0x0F) | 0x70;
        bytes[8] = (bytes[8] & 0x3F) | 0x80;
        Self { data: bytes }
    }
}

impl Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.data))
    }
}

impl Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.data))
    }
}

impl FromStr for Context {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut data = [0u8; 16];
        hex::decode_to_slice(s, &mut data)?;
        Ok(Self { data })
    }
}

impl From<[u8; 16]> for Context {
    fn from(data: [u8; 16]) -> Self {
        Self { data }
    }
}

impl From<&[u8; 16]> for Context {
    fn from(data: &[u8; 16]) -> Self {
        Self { data: *data }
    }
}

impl From<&[u8]> for Context {
    fn from(slice: &[u8]) -> Self {
        let mut data = [0u8; 16];
        let len = slice.len().min(16);
        data[..len].copy_from_slice(&slice[..len]);
        Self { data }
    }
}

impl From<Bytes> for Context {
    fn from(bytes: Bytes) -> Self {
        Self::from(bytes.as_ref())
    }
}

impl From<Context> for [u8; 16] {
    fn from(ctx: Context) -> Self {
        ctx.data
    }
}

impl AsRef<[u8]> for Context {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

// ============================================================================
// Hash (32 bytes BLAKE3)
// ============================================================================

#[repr(C)]
#[derive(
    Copy,
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    std::hash::Hash,
    IntoBytes,
    FromBytes,
    Immutable,
    KnownLayout,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct Hash {
    pub data: [u8; 32],
}

impl Hash {
    pub const ZERO: Self = Self { data: [0u8; 32] };

    pub fn is_zero(&self) -> bool {
        self.data == [0u8; 32]
    }

    pub fn from_context(ctx: Context) -> Self {
        let mut data = [0u8; 32];
        data[..16].copy_from_slice(&ctx.data);
        Self { data }
    }

    pub fn to_context(&self) -> Context {
        let mut data = [0u8; 16];
        data.copy_from_slice(&self.data[..16]);
        Context { data }
    }

    pub fn from_u64(val: u64) -> Self {
        let mut data = [0u8; 32];
        data[..8].copy_from_slice(&val.to_le_bytes());
        Self { data }
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.data))
    }
}

impl Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.data))
    }
}

impl FromStr for Hash {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut data = [0u8; 32];
        hex::decode_to_slice(s, &mut data)?;
        Ok(Self { data })
    }
}

impl From<[u8; 32]> for Hash {
    fn from(data: [u8; 32]) -> Self {
        Self { data }
    }
}

impl From<&[u8; 32]> for Hash {
    fn from(data: &[u8; 32]) -> Self {
        Self { data: *data }
    }
}

impl From<&[u8]> for Hash {
    fn from(slice: &[u8]) -> Self {
        let mut data = [0u8; 32];
        let len = slice.len().min(32);
        data[..len].copy_from_slice(&slice[..len]);
        Self { data }
    }
}

impl From<Bytes> for Hash {
    fn from(bytes: Bytes) -> Self {
        Self::from(bytes.as_ref())
    }
}

impl From<Hash> for [u8; 32] {
    fn from(hash: Hash) -> Self {
        hash.data
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

// ============================================================================
// Address (48 bytes = 32B Hash + 16B Context)
// ============================================================================

#[repr(C)]
#[derive(
    Copy,
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    std::hash::Hash,
    IntoBytes,
    FromBytes,
    Immutable,
    KnownLayout,
)]
pub struct Address {
    pub hash: Hash,
    pub context: Context,
}

impl Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.hash, self.context)
    }
}

impl Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Address({}:{})", self.hash, self.context)
    }
}

impl From<&[u8]> for Address {
    fn from(slice: &[u8]) -> Self {
        let mut hash = [0u8; 32];
        let mut context = [0u8; 16];
        if slice.len() >= 48 {
            hash.copy_from_slice(&slice[0..32]);
            context.copy_from_slice(&slice[32..48]);
        }
        Self {
            hash: Hash { data: hash },
            context: Context { data: context },
        }
    }
}

impl From<Bytes> for Address {
    fn from(bytes: Bytes) -> Self {
        Self::from(bytes.as_ref())
    }
}

// ============================================================================
// Fragment (16 bytes = 4B flags + 4B size_payload + 8B size_content)
// ============================================================================

#[repr(C)]
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    IntoBytes,
    FromBytes,
    Immutable,
    KnownLayout,
    Serialize,
    Deserialize,
)]
pub struct Fragment {
    pub flags: u32,
    pub size_payload: u32,
    pub size_content: u64,
}

impl From<&[u8]> for Fragment {
    fn from(slice: &[u8]) -> Self {
        if slice.len() >= 16 {
            let flags = u32::from_le_bytes(slice[0..4].try_into().unwrap());
            let size_payload = u32::from_le_bytes(slice[4..8].try_into().unwrap());
            let size_content = u64::from_le_bytes(slice[8..16].try_into().unwrap());
            Self {
                flags,
                size_payload,
                size_content,
            }
        } else {
            Self::default()
        }
    }
}

impl From<Bytes> for Fragment {
    fn from(bytes: Bytes) -> Self {
        Self::from(bytes.as_ref())
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, IntoBytes, Immutable, FromBytes, KnownLayout)]
pub struct FragmentReference {
    pub hash: Hash,
    pub offset_content: u64,
}

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct FragmentFlags: u32 {
        const NoFlags = 0;
        const PayloadFragmented = 0b1;
        const PayloadCompressedLZ4 = 0b10;
        const PayloadCompressedOodle2 = 0b100;
        const PayloadCompressedZstd = 0b1000;
        const PayloadCompressed = 0b11111110;
        const PayloadObliterated = 0b100000000;
        const PayloadObliterating = 0b1000000000;
        const PayloadObliteration = 0b1100000000;
        const PayloadLocalCachePriority = 0b10000000000000000;
        const PayloadRevisionState = 0b100000000000000000;
        const PayloadStoredDurable = 0b1000000000000000000;
        const PayloadStoredLocal = 0b100000000000000000000;
        const PayloadStored = 0b111000000000000000000;
        const PayloadDoNotReplicate = 0b1000000000000000000000;
    }
}

impl BitOr<FragmentFlags> for u32 {
    type Output = u32;
    fn bitor(self, rhs: FragmentFlags) -> Self::Output {
        self | rhs.bits()
    }
}

impl BitAnd<FragmentFlags> for u32 {
    type Output = u32;
    fn bitand(self, rhs: FragmentFlags) -> Self::Output {
        self & rhs.bits()
    }
}

impl BitOrAssign<FragmentFlags> for u32 {
    fn bitor_assign(&mut self, rhs: FragmentFlags) {
        *self |= rhs.bits();
    }
}

impl BitAndAssign<FragmentFlags> for u32 {
    fn bitand_assign(&mut self, rhs: FragmentFlags) {
        *self &= rhs.bits();
    }
}

// ============================================================================
// KeyType & Store Types
// ============================================================================

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, std::hash::Hash)]
pub enum KeyType {
    #[default]
    Untyped = 0,
    BranchMetadata = 1,
    BranchId = 2,
    BranchLatestPointer = 3,
    RepositoryMetadata = 4,
    RepositoryId = 5,
    Instance = 6,
    Resolve = 7,
}

impl TryFrom<u8> for KeyType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Untyped),
            1 => Ok(Self::BranchMetadata),
            2 => Ok(Self::BranchId),
            3 => Ok(Self::BranchLatestPointer),
            4 => Ok(Self::RepositoryMetadata),
            5 => Ok(Self::RepositoryId),
            6 => Ok(Self::Instance),
            7 => Ok(Self::Resolve),
            _ => bail!("invalid KeyType: {value}"),
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum HealResult {
    #[default]
    NotAttempted = 0,
    Healed = 1,
    Failed = 2,
}

// ============================================================================
// BLAKE3 Hashing & LZ4 / Zstd Decompression
// ============================================================================

pub fn hash_slice(data: &[u8]) -> Hash {
    let digest = blake3::hash(data);
    Hash {
        data: *digest.as_bytes(),
    }
}

pub fn hash_string(s: &str) -> Hash {
    hash_slice(s.as_bytes())
}

pub fn hash_function(salt: &[u8], function: &str) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(salt);
    hasher.update(function.as_bytes());
    Hash {
        data: *hasher.finalize().as_bytes(),
    }
}

pub fn hash_function_arg(salt: &[u8], function: &str, arg: &str) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(salt);
    hasher.update(function.as_bytes());
    hasher.update(arg.as_bytes());
    Hash {
        data: *hasher.finalize().as_bytes(),
    }
}

pub fn hash_function_args(salt: &[u8], function: &str, arg1: &str, arg2: &str) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(salt);
    hasher.update(function.as_bytes());
    hasher.update(arg1.as_bytes());
    hasher.update(arg2.as_bytes());
    Hash {
        data: *hasher.finalize().as_bytes(),
    }
}

/// Decompresses a fragment payload if `PayloadCompressedLZ4` or `PayloadCompressedZstd` is set.
/// Returns the uncompressed bytes.
pub fn decompress_fragment(fragment: Fragment, data: &[u8]) -> Result<Vec<u8>> {
    if (fragment.flags & FragmentFlags::PayloadCompressed.bits()) == 0 {
        return Ok(data.to_vec());
    }

    let size_content = fragment.size_content as usize;
    if size_content > FRAGMENT_SIZE_THRESHOLD * 64 {
        bail!("compressed fragment size_content {size_content} exceeds safety limit");
    }

    if (fragment.flags & FragmentFlags::PayloadCompressedLZ4.bits()) != 0 {
        let mut out = vec![0u8; size_content];
        let decoded = unsafe {
            lz4_sys::LZ4_decompress_safe(
                data.as_ptr() as *const std::ffi::c_char,
                out.as_mut_ptr() as *mut std::ffi::c_char,
                data.len() as std::ffi::c_int,
                size_content as std::ffi::c_int,
            )
        };
        if decoded < 0 || decoded as usize != size_content {
            bail!("LZ4_decompress_safe failed: returned {decoded}, expected {size_content}");
        }
        return Ok(out);
    }

    if (fragment.flags & FragmentFlags::PayloadCompressedZstd.bits()) != 0 {
        let mut out = vec![0u8; size_content];
        let decoded = unsafe {
            zstd_sys::ZSTD_decompress(
                out.as_mut_ptr() as *mut std::ffi::c_void,
                size_content,
                data.as_ptr() as *const std::ffi::c_void,
                data.len(),
            )
        };
        if unsafe { zstd_sys::ZSTD_isError(decoded) } != 0 || decoded != size_content {
            bail!("ZSTD_decompress failed: returned {decoded}, expected {size_content}");
        }
        return Ok(out);
    }

    bail!(
        "unsupported compression flags: {:#x}",
        fragment.flags & FragmentFlags::PayloadCompressed.bits()
    )
}

/// Computes the canonical Lore content hash for a `Fragment` and its wire `payload`.
/// For uncompressed fragments (including `PayloadFragmented` reference lists), hashes `payload`
/// directly. For compressed fragments, decompresses first and hashes the uncompressed content.
pub fn hash_fragment(fragment: Fragment, data: &[u8]) -> Result<Hash> {
    if (fragment.flags & FragmentFlags::PayloadCompressed.bits()) == 0 {
        Ok(hash_slice(data))
    } else {
        let decompressed = decompress_fragment(fragment, data)?;
        Ok(hash_slice(&decompressed))
    }
}

pub fn validate_fragment_metadata(fragment: &Fragment) -> Result<()> {
    if fragment.size_payload == 0 {
        bail!("fragment size_payload is 0");
    }
    if (fragment.size_payload as usize) > FRAGMENT_SIZE_THRESHOLD {
        bail!(
            "fragment size_payload {} exceeds FRAGMENT_SIZE_THRESHOLD {}",
            fragment.size_payload,
            FRAGMENT_SIZE_THRESHOLD
        );
    }
    if (fragment.size_payload as u64) > fragment.size_content {
        bail!("fragment size_payload exceeds size_content");
    }
    let is_fragmented = (fragment.flags & FragmentFlags::PayloadFragmented.bits()) != 0;
    let is_compressed = (fragment.flags & FragmentFlags::PayloadCompressed.bits()) != 0;
    if is_fragmented && is_compressed {
        bail!("fragment cannot be both fragmented and compressed");
    }
    if !is_fragmented && !is_compressed && (fragment.size_payload as u64) != fragment.size_content {
        bail!("uncompressed, unfragmented fragment must have size_payload == size_content");
    }
    Ok(())
}

pub fn validate_fragment_list(fragment: &Fragment, payload: &[u8]) -> Result<()> {
    let ref_size = std::mem::size_of::<FragmentReference>();
    if payload.is_empty() || !payload.len().is_multiple_of(ref_size) {
        bail!("invalid fragment list payload length {}", payload.len());
    }
    if payload.len() != fragment.size_payload as usize {
        bail!("fragment list payload length does not match size_payload");
    }
    Ok(())
}

// ============================================================================
// Lore Binary `Metadata` Blob Reader / Writer (`magic = 0x6D657461`, `version = 1`)
// ============================================================================

const METADATA_MAGIC: u32 = 0x6D657461;
const METADATA_VERSION: u32 = 1;

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MetadataValueType {
    Address = 1,
    Boolean = 2,
    Context = 3,
    Hash = 4,
    Numeric = 5,
    String = 6,
    Binary = 255,
}

impl TryFrom<u32> for MetadataValueType {
    type Error = anyhow::Error;

    fn try_from(value: u32) -> Result<Self> {
        match value {
            1 => Ok(Self::Address),
            2 => Ok(Self::Boolean),
            3 => Ok(Self::Context),
            4 => Ok(Self::Hash),
            5 => Ok(Self::Numeric),
            6 => Ok(Self::String),
            255 => Ok(Self::Binary),
            _ => bail!("unknown MetadataValueType {value}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataEntry {
    pub key: String,
    pub value_type: MetadataValueType,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataBlob {
    pub entries: Vec<MetadataEntry>,
}

impl MetadataBlob {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 8 {
            bail!("MetadataBlob buffer too short ({} bytes)", buf.len());
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if magic != METADATA_MAGIC || version != METADATA_VERSION {
            bail!("invalid MetadataBlob header: magic={magic:#x}, version={version}");
        }

        let mut offset = 8;
        let mut entries = Vec::new();
        while offset < buf.len() {
            if offset + 12 > buf.len() {
                bail!("truncated MetadataItem header at offset {offset}");
            }
            let key_len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
            let val_len =
                u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap()) as usize;
            let val_type_raw = u32::from_le_bytes(buf[offset + 8..offset + 12].try_into().unwrap());
            let value_type = MetadataValueType::try_from(val_type_raw)?;
            offset += 12;

            if offset + key_len + val_len > buf.len() {
                bail!("truncated MetadataItem payload at offset {offset}");
            }
            let key = String::from_utf8(buf[offset..offset + key_len].to_vec())
                .map_err(|e| anyhow!("invalid UTF-8 in MetadataItem key: {e}"))?;
            offset += key_len;
            let value = buf[offset..offset + val_len].to_vec();
            offset += val_len;

            entries.push(MetadataEntry {
                key,
                value_type,
                value,
            });
        }

        Ok(Self { entries })
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut total_size = 8;
        for entry in &self.entries {
            total_size += 12 + entry.key.len() + entry.value.len();
        }
        let mut out = Vec::with_capacity(total_size);
        out.extend_from_slice(&METADATA_MAGIC.to_le_bytes());
        out.extend_from_slice(&METADATA_VERSION.to_le_bytes());

        for entry in &self.entries {
            out.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
            out.extend_from_slice(&(entry.value.len() as u32).to_le_bytes());
            out.extend_from_slice(&(entry.value_type as u32).to_le_bytes());
            out.extend_from_slice(entry.key.as_bytes());
            out.extend_from_slice(&entry.value);
        }

        Bytes::from(out)
    }

    pub fn set_raw(&mut self, key: &str, value_type: MetadataValueType, value: Vec<u8>) {
        if let Some(existing) = self.entries.iter_mut().find(|e| e.key == key) {
            existing.value_type = value_type;
            existing.value = value;
        } else {
            self.entries.push(MetadataEntry {
                key: key.to_string(),
                value_type,
                value,
            });
        }
    }

    pub fn get_raw(&self, key: &str, expected_type: MetadataValueType) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|e| e.key == key && e.value_type == expected_type)
            .map(|e| e.value.as_slice())
    }

    pub fn set_string(&mut self, key: &str, val: &str) {
        self.set_raw(key, MetadataValueType::String, val.as_bytes().to_vec());
    }

    pub fn get_string(&self, key: &str) -> Option<String> {
        self.get_raw(key, MetadataValueType::String)
            .and_then(|b| std::str::from_utf8(b).ok().map(ToOwned::to_owned))
    }

    pub fn set_numeric(&mut self, key: &str, val: u64) {
        self.set_raw(key, MetadataValueType::Numeric, val.to_le_bytes().to_vec());
    }

    pub fn get_numeric(&self, key: &str) -> Option<u64> {
        let b = self.get_raw(key, MetadataValueType::Numeric)?;
        if b.len() == 8 {
            Some(u64::from_le_bytes(b.try_into().unwrap()))
        } else {
            None
        }
    }

    pub fn set_boolean(&mut self, key: &str, val: bool) {
        self.set_raw(key, MetadataValueType::Boolean, vec![u8::from(val)]);
    }

    pub fn get_boolean(&self, key: &str) -> Option<bool> {
        let b = self.get_raw(key, MetadataValueType::Boolean)?;
        b.first().map(|&v| v != 0)
    }

    pub fn set_context(&mut self, key: &str, val: Context) {
        self.set_raw(key, MetadataValueType::Context, val.data.to_vec());
    }

    pub fn get_context(&self, key: &str) -> Option<Context> {
        let b = self.get_raw(key, MetadataValueType::Context)?;
        if b.len() == 16 {
            Some(Context::from(b))
        } else {
            None
        }
    }

    pub fn set_hash(&mut self, key: &str, val: Hash) {
        self.set_raw(key, MetadataValueType::Hash, val.data.to_vec());
    }

    pub fn get_hash(&self, key: &str) -> Option<Hash> {
        let b = self.get_raw(key, MetadataValueType::Hash)?;
        if b.len() == 32 {
            Some(Hash::from(b))
        } else {
            None
        }
    }

    pub fn set_binary(&mut self, key: &str, val: &[u8]) {
        self.set_raw(key, MetadataValueType::Binary, val.to_vec());
    }

    pub fn get_binary(&self, key: &str) -> Option<&[u8]> {
        self.get_raw(key, MetadataValueType::Binary)
    }
}

// ============================================================================
// Lore `StateData` (320-byte Commit State Header) Parser
// ============================================================================

pub const STATE_MAGIC: u32 = 0xD37A208E;
pub const STATE_DATA_SIZE: usize = 320;

#[derive(Clone, Debug)]
pub struct StateDataView {
    pub revision_number: u64,
    pub parent_self: Hash,
    pub parent_other: Hash,
    pub hash_tree: Hash,
    pub hash_metadata: Hash,
    pub hash_link: Hash,
    pub parent_repository: RepositoryId,
    pub raw_bytes: Bytes,
}

impl StateDataView {
    pub fn parse(fragment: Fragment, payload: &[u8]) -> Result<Self> {
        let decompressed = decompress_fragment(fragment, payload)?;
        if decompressed.len() < STATE_DATA_SIZE {
            bail!(
                "StateData too small: {} bytes (expected {})",
                decompressed.len(),
                STATE_DATA_SIZE
            );
        }
        let magic = u32::from_le_bytes(decompressed[0..4].try_into().unwrap());
        if magic != STATE_MAGIC {
            bail!("invalid StateData magic {magic:#x}");
        }
        let revision_number = u64::from_le_bytes(decompressed[24..32].try_into().unwrap());
        let parent_self = Hash::from(&decompressed[32..64]);
        let parent_other = Hash::from(&decompressed[64..96]);
        let hash_tree = Hash::from(&decompressed[96..128]);
        let hash_metadata = Hash::from(&decompressed[128..160]);
        let hash_link = Hash::from(&decompressed[160..192]);
        let parent_repository = RepositoryId::from(&decompressed[256..272]);

        Ok(Self {
            revision_number,
            parent_self,
            parent_other,
            hash_tree,
            hash_metadata,
            hash_link,
            parent_repository,
            raw_bytes: Bytes::from(decompressed),
        })
    }
}
