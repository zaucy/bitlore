# AGENTS.md — Bitlore Developer & AI Agent Guide

This file provides essential architectural context, invariants, and verification workflows for AI coding agents and developers working in the `bitlore` repository.

---

## 1. Core Constraints (Non-Negotiable)

1. **100% Standalone Repository**:
   - `bitlore` must **never** depend on any `lore-*` crates or path dependencies from the upstream `lore` repository.
   - `bitlore` implements the Lore wire protocols (QUIC storage v2/v4 and gRPC discovery/auth/revision) directly.
2. **Zero Modifications to Upstream `lore`**:
   - Never modify the upstream `lore` client or server codebase to accommodate `bitlore`.
3. **100% Vanilla `lore` Client Compatibility**:
   - Every feature in `bitlore-server` and `bitlore-peer` must work seamlessly with an unmodified vanilla `lore` CLI binary (`lore repository create`, `lore stage`, `lore commit`, `lore branch push`, `lore clone`, `lore sync`).

---

## 2. Repository Structure & Module Map

| File | Responsibility |
| :--- | :--- |
| [`src/lib.rs`](src/lib.rs) | Crate root, module exports, and background task helper (`spawn_bg`). |
| [`src/main.rs`](src/main.rs) | Entrypoint for `bitlore-server` (defaults to `immutable_store.mode = "bitlore"`). |
| [`src/bin/bitlore_peer.rs`](src/bin/bitlore_peer.rs) | Entrypoint for `bitlore-peer` (defaults to `immutable_store.mode = "bitlore-peer"`). |
| [`src/types.rs`](src/types.rs) | Binary-compatible Lore types (`Context`, `Partition`, `Hash`, `Address`, `Fragment`, `FragmentReference`, `FragmentFlags`, `KeyType`), BLAKE3 `"urc"` domain hashing, LZ4/Zstd decompression, `MetadataBlob`, and `StateDataView`. |
| [`src/local_store.rs`](src/local_store.rs) | `ImmutableStore` & `MutableStore` traits, `StoreError`, `StoreMatch`, and on-disk `LocalImmutableStore` / `LocalMutableStore` implementations. |
| [`src/quic_server.rs`](src/quic_server.rs) | Native `quinn` QUIC storage server supporting `lore-storage/0.4` (12-byte v4 headers) and `urc/0.2` (8-byte v2 headers), self-signed TLS generation, and all storage opcodes. |
| [`src/grpc_server.rs`](src/grpc_server.rs) | Custom `prost` + `tonic` gRPC server implementing `ServiceDiscovery`, `AuthenticationService`, and `RevisionService`, plus TOML config loading. |
| [`src/store.rs`](src/store.rs) | `BitloreMetadataStore` — separates structural fragments (`context == 0`) from file content fragments (`context != 0`) for metadata-only hosting. |
| [`src/peer_store.rs`](src/peer_store.rs) | `BitlorePeerStore` — local P2P workstation store that forwards structural fragments to `bitlore-server`, announces file ownership to the tracker, and fetches missing file chunks over `iroh`. |
| [`src/tracker.rs`](src/tracker.rs) | `/bitlore.p2p.v1.P2pTracker` gRPC server (`BitloreP2pTrackerState`) and client (`BitloreTrackerClient`). |
| [`src/p2p_node.rs`](src/p2p_node.rs) | `BitloreIrohNode` — `iroh::Endpoint` wrapper handling hole-punched QUIC P2P chunk transfers (`ALPN = bitlore/p2p-chunk/0.1`). |
| [`config/default.toml`](config/default.toml) | Default configuration for `bitlore-server`. |
| [`scripts/e2e_verify.ps1`](scripts/e2e_verify.ps1) | End-to-end verification script for metadata-only hosting with vanilla `lore.exe`. |
| [`scripts/e2e_p2p_verify.ps1`](scripts/e2e_p2p_verify.ps1) | End-to-end verification script for 3-peer P2P swarm + tracker + `iroh` with vanilla `lore.exe`. |

---

## 3. Critical Wire-Protocol Invariants

When modifying [`src/types.rs`](src/types.rs), [`src/quic_server.rs`](src/quic_server.rs), or [`src/grpc_server.rs`](src/grpc_server.rs), preserve these exact wire invariants:

1. **Structural vs. File Fragment Rule (`Address.context`)**:
   - `address.context.is_zero()` (`[0u8; 16]`) **always** denotes a structural/metadata fragment (`StateData`, `Tree`, `NodeBlock`, `NameTable`, `NodeFileMetadataBlock`, `Metadata`).
   - `!address.context.is_zero()` **always** denotes a file content fragment (`context` is the file's 16-byte UUIDv7 `file_id`).
2. **`FragmentFlags` Bit Positions ([`src/types.rs`](src/types.rs))**:
   - `PayloadFragmented = 0b1` (bit 0)
   - `PayloadCompressedLZ4 = 0b10` (bit 1)
   - `PayloadCompressedOodle2 = 0b100` (bit 2)
   - `PayloadCompressedZstd = 0b1000` (bit 3)
   - `PayloadCompressed = 0b11111110` (bits 1..=7)
   - `PayloadLocalCachePriority = 1 << 16`
   - `PayloadRevisionState = 1 << 17`
   - `PayloadStoredDurable = 1 << 18`
   - `PayloadStoredLocal = 1 << 20`
   - `PayloadStored = 0b111 << 18`
   - `PayloadDoNotReplicate = 1 << 21`
3. **`KeyType` Discriminants ([`src/types.rs`](src/types.rs))**:
   - `Untyped = 0`, `BranchMetadata = 1`, `BranchId = 2`, `BranchLatestPointer = 3`, `RepositoryMetadata = 4`, `RepositoryId = 5`, `Instance = 6`, `Resolve = 7`.
4. **Content Hashing (`hash_fragment`)**:
   - Lore hashes the **uncompressed** content using BLAKE3 (except for `PayloadFragmented` reference lists, which are never compressed and are hashed over their raw 40-byte `FragmentReference` array). Always decompress LZ4/Zstd payloads before comparing against `address.hash`.
5. **Mutable Store Key Derivation (`SALT_LORE = b"urc"`)**:
   - Repository and branch keys in `RevisionService` use BLAKE3 with `b"urc"` salt (`hash_function_arg(b"urc", "repository-name", name)`, `hash_function_args(b"urc", "branch-name", &repo.to_string(), name)`, etc.).

---

## 4. Build & Verification Workflow

Always run all three checks after making code changes:

```powershell
# 1. Compile and run unit tests
cargo test

# 2. Build both binaries
cargo build --bins

# 3. Run both E2E verification scripts against vanilla lore.exe
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/e2e_verify.ps1
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/e2e_p2p_verify.ps1
```
