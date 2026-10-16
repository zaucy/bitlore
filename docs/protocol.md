# Bitlore Wire Protocols & Data Formats

Bitlore is 100% standalone and implements the Lore wire protocols directly without depending on any `lore-*` crates. This document details the binary formats, QUIC framing, gRPC services, and P2P protocols implemented in the codebase.

---

## 1. Core Binary Primitives ([`src/types.rs`](../src/types.rs))

All wire structs are `#[repr(C)]` and little-endian.

### Identifiers & Addresses
- **`Context` / `Partition` / `RepositoryId` / `BranchId` (16 bytes)**: Raw 128-bit identifier (`[u8; 16]`).
- **`Hash` (32 bytes)**: BLAKE3 256-bit digest (`[u8; 32]`).
  - Domain-salted mutable store keys use `SALT_LORE = b"urc"` hashed with function names and arguments via BLAKE3 (`hash_function`, `hash_function_arg`, `hash_function_args`).
- **`Address` (48 bytes)**:
  - Bytes `0..32`: `hash: Hash` (BLAKE3 digest of the uncompressed content, or of the `FragmentReference` array if `PayloadFragmented` is set).
  - Bytes `32..48`: `context: Context` (`Context::ZERO` for structural fragments, non-zero `file_id` for file content fragments).

### `Fragment` Header (16 bytes)
| Offset | Size | Field | Type | Description |
| :--- | :--- | :--- | :--- | :--- |
| `0..4` | 4B | `flags` | `u32` (LE) | Bitmask of [`FragmentFlags`](../src/types.rs) |
| `4..8` | 4B | `size_payload` | `u32` (LE) | Wire/stored byte length of the fragment payload (`<= 256 KiB`) |
| `8..16` | 8B | `size_content` | `u64` (LE) | Uncompressed and reassembled content byte length |

### `FragmentFlags` (`u32`)
The exact bit positions must match `lore-base`:
- `PayloadFragmented = 0b1` (`1 << 0`): Payload is a contiguous array of 40-byte `FragmentReference`s (`Hash` (32B) + `offset_content: u64` (8B)).
- `PayloadCompressedLZ4 = 0b10` (`1 << 1`): Payload is compressed with raw LZ4 block compression.
- `PayloadCompressedOodle2 = 0b100` (`1 << 2`): Payload is compressed with Oodle2.
- `PayloadCompressedZstd = 0b1000` (`1 << 3`): Payload is compressed with Zstandard.
- `PayloadCompressed = 0b11111110` (bits 1–7).
- `PayloadObliterated = 1 << 8`, `PayloadObliterating = 1 << 9`.
- `PayloadLocalCachePriority = 1 << 16` (`0x1_0000`).
- `PayloadRevisionState = 1 << 17` (`0x2_0000`).
- `PayloadStoredDurable = 1 << 18` (`0x4_0000`).
- `PayloadStoredLocal = 1 << 20` (`0x10_0000`).
- `PayloadStored = 0b111 << 18` (`0x1C_0000`).
- `PayloadDoNotReplicate = 1 << 21` (`0x20_0000`).

### `KeyType` (`u8`)
Used in mutable key-value store operations (`MutableLoad`, `MutableStore`, `MutableCas`):
- `0`: `Untyped`
- `1`: `BranchMetadata`
- `2`: `BranchId`
- `3`: `BranchLatestPointer`
- `4`: `RepositoryMetadata`
- `5`: `RepositoryId`
- `6`: `Instance`
- `7`: `Resolve`

---

## 2. Lore Binary Metadata & Commit State Formats ([`src/types.rs`](../src/types.rs))

### `MetadataBlob` (`magic = 0x6D657461` / `"meta"`, `version = 1`)
- **Header (8 bytes)**: `magic: u32` (`0x6D657461` LE) + `version: u32` (`1` LE).
- **Repeated `MetadataItem`s**:
  - `key_len: u32` (LE)
  - `val_len: u32` (LE)
  - `value_type: u32` (LE): `1 = Address`, `2 = Boolean`, `3 = Context`, `4 = Hash`, `5 = Numeric`, `6 = String`, `255 = Binary`
  - `key`: `key_len` UTF-8 bytes
  - `value`: `val_len` raw bytes

### `StateDataView` (320-byte Commit State Header, `magic = 0xD37A208E`)
Stored as a structural fragment (`context == 0`, often Zstd-compressed):
- `0..4`: `magic: u32` (`0xD37A208E`)
- `24..32`: `revision_number: u64`
- `32..64`: `parent[0]: Hash` (`parent_self`)
- `64..96`: `parent[1]: Hash` (`parent_other`)
- `96..128`: `hash_tree: Hash`
- `128..160`: `hash_metadata: Hash`
- `160..192`: `hash_link: Hash`
- `256..272`: `parent_repository: RepositoryId`

---

## 3. Native QUIC Storage Protocol ([`src/quic_server.rs`](../src/quic_server.rs))

Bitlore's QUIC server listens on the same UDP port as the gRPC TCP port (default `42337`) and negotiates ALPNs `lore-storage/0.4` (v4) and `urc/0.2` (v2).

### CommandHeader Framing
- **v2 (`urc/0.2`, 8 bytes)**:
  - Byte `0`: `cmd: u8`
  - Bytes `1..4`: 23-bit `size_or_status: u32` (LE) + bit 23 (`0x80` on byte 3) as `error: bool`
  - Bytes `4..8`: `command_id: u32` (LE)
- **v4 (`lore-storage/0.4`, 12 bytes)**:
  - Bytes `0..8`: Same as v2
  - Bytes `8..12`: `session_id: u32` (LE)

### Opcodes (`QuicCommand`)
| Opcode | Name | Request Payload | Response Payload |
| :--- | :--- | :--- | :--- |
| `0` | `Authorize` | v4 start: `0u8 ++ repo(16) ++ corr_len(1) ++ corr ++ tok_len(2) ++ tok`<br>v4 stop: `1u8` | v4 start: `session_id: u32` (LE)<br>v4 stop: empty |
| `1` | `Get` | `Address` (48B) | `Fragment` (16B) ++ `payload` |
| `2` | `Put` | `Address` (48B) ++ `Fragment` (16B) ++ `payload` | empty |
| `3` | `Query` | `N * Address` (`N * 48B`) | `N * u8` (`0` = Full, `1` = Partition, `3` = NotFound) |
| `5` | `CorrelateLegacy` | Correlation string | empty |
| `6` | `Verify` | `Address` (48B) ++ `heal: u8` (1B) | `[corrupted: u8, healed: u8]` (2B) |
| `7` | `Copy` | v4 (80B): `src_repo(16) ++ Hash(32) ++ src_ctx(16) ++ dst_ctx(16)` | empty |
| `8` | `MutableLoad` | `key: Hash` (32B) ++ `key_type: u8` (1B) | `value: Hash` (32B) |
| `9` | `MutableStore` | `key: Hash` (32B) ++ `value: Hash` (32B) ++ `key_type: u8` (1B) | empty |
| `10` | `MutableCas` | `key(32) ++ expected(32) ++ value(32) ++ key_type(1)` | `current: Hash` (32B) |
| `11` | `GetMetadata` | `Address` (48B) | `Fragment` (16B) |
| `12` | `GetResolved` | `key: Hash` (32B) ++ `Context` (16B) ++ `flags: u32` (4B) | `resolved: Hash` (32B) ++ `Fragment` (16B) ++ `payload` |
| `13` | `PutResolved` | `key: Hash` (32B) ++ `Address` (48B) ++ `Fragment` (16B) ++ `payload` | empty |
| `14` | `ClientIdentify` | Length-prefixed user-agent string | empty |

---

## 4. Native gRPC Services ([`src/grpc_server.rs`](../src/grpc_server.rs))

Implemented via a custom `tonic` codec (`ProstCodec`) with zero `.proto` build dependencies:

- `lore.public.protocol.discovery.v1.ServiceDiscovery/GetEndpoints`
- `lore.public.protocol.auth.v1.AuthenticationService/GetConfiguration`, `GetIdentityToken`, `GetAuthorizationToken`
- `lore.public.protocol.revision.v2.RevisionService`:
  - `RepositoryCreate`, `RepositoryDelete`, `RepositoryList`, `RepositoryResolveId`, `RepositoryMetadataGet`, `RepositoryMetadataSet`
  - `BranchCreate`, `BranchDelete`, `BranchList`, `BranchResolveId`, `BranchMetadataGet`, `BranchMetadataSet`, `BranchPush`
  - `RevisionList`, `RevisionResolve`
  - `Lock`, `Unlock`, `LockAdmin`, `LockList`, `LockRefresh`, `LockStatus`

---

## 5. Bitlore P2P Tracker & `iroh` Chunk Swarm Protocols

### P2P Tracker gRPC (`/bitlore.p2p.v1.P2pTracker` in [`src/tracker.rs`](../src/tracker.rs))
- `Announce(AnnounceRequest) -> AnnounceResponse`: Registers a peer's `iroh::EndpointId`, JSON-serialized `iroh::EndpointAddr` (direct UDP addresses + relay URL), and `(repository_id, file_id)` ownership list.
- `Resolve(ResolveRequest) -> ResolveResponse`: Returns active peers holding `(repository_id, file_id)` (ordered by exact file match first, then repository-level peers, excluding expired peers past `peer_ttl`).
- `Heartbeat(HeartbeatRequest) -> HeartbeatResponse`: Refreshes peer liveness; returns `known = false` if the tracker restarted so the peer immediately re-announces its inventory.
- `QueryStructural`, `PutStructural`, `GetStructural`: Used by `bitlore-peer` sidecars to forward structural fragments (`context == 0`) to and from `bitlore-server`.

### `iroh` P2P Chunk Protocol (`ALPN = bitlore/p2p-chunk/0.1` in [`src/p2p_node.rs`](../src/p2p_node.rs))
Each P2P chunk fetch opens a single bidirectional QUIC stream between two `bitlore-peer` nodes:
- **Request (64 bytes)**: `repository_id` (16B) ++ `hash` (32B) ++ `context` (16B)
- **Response**:
  - Status `0u8` (Found): `0u8` (1B) ++ `Fragment` (16B) ++ `payload` (`size_payload` bytes)
  - Status `1u8` (Not Found): `1u8` (1B)
  - Status `2u8` (Internal Error): `2u8` (1B)
