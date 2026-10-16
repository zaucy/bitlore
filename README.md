# Bitlore

**Bitlore** is a standalone, metadata-only hosting server (`bitlore-server`) and peer-to-peer asset swarm sidecar (`bitlore-peer`) for the **Lore** version control system.

It allows game studios and distributed teams to host repositories, branches, commit history, locks, and directory trees on a lightweight central server (~1–5 KB per commit) while transferring multi-gigabyte binary assets directly between teammates over hole-punched QUIC ([`iroh`](https://iroh.computer)).

- **100% Vanilla Client Compatible**: Works out-of-the-box with unmodified `lore` CLI binaries (`lore commit`, `lore branch push`, `lore clone`, `lore sync`).
- **100% Standalone**: Zero dependencies on internal `lore-*` crates; implements Lore's QUIC storage (`lore-storage/0.4`, `urc/0.2`) and gRPC (`ServiceDiscovery`, `AuthenticationService`, `RevisionService`) wire protocols directly.
- **NAT-Traversed P2P Swarm**: Uses `iroh` endpoints with automatic relay fallback and BLAKE3 chunk verification so teammates seed file chunks to each other as soon as they fetch them.

---

## Quick Start

### 1. Build Binaries

```powershell
cargo build --bins
```

This produces two binaries in `target/debug/`:
- `bitlore-server.exe` — Metadata-only Lore server + P2P file ownership tracker
- `bitlore-peer.exe` — Workstation P2P sidecar + `iroh` chunk seeder/fetcher

### 2. Run Unit & End-to-End Tests

```powershell
# Run Rust unit tests (metadata separation + 2-peer iroh swarm transfer)
cargo test

# Run E2E metadata-only verification with vanilla lore.exe
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/e2e_verify.ps1

# Run E2E 3-peer P2P swarm + tracker + iroh verification with vanilla lore.exe
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/e2e_p2p_verify.ps1
```

---

## How It Works

Every immutable fragment in Lore is addressed by a 48-byte `Address { hash: Hash, context: Context }`:

1. **Structural & Merkle Tree Fragments (`address.context == 0`)**:
   - Includes commit `StateData`, directory `Tree`s, `NodeBlock`s, `NameTable`s, and commit/branch/repository `Metadata`.
   - Stored durably on `bitlore-server` (~4 KB for an entire commit).
2. **File Content Fragments (`address.context != 0`)**:
   - Every file content chunk carries the file's non-zero 16-byte UUIDv7 `file_id` in `address.context`.
   - `bitlore-server` never stores file bytes on disk (`store_file_payloads = false`). Instead, `bitlore-peer` sidecars announce `(repository_id, file_id)` ownership to `bitlore-server`'s P2P tracker and transfer verified chunks directly peer-to-peer over `iroh` QUIC.

---

## Documentation

- [**Architecture Overview (`docs/architecture.md`)**](docs/architecture.md) — Structural vs. file fragment separation, `ServiceDiscovery` routing, and P2P swarm sequence diagrams.
- [**Wire Protocols & Data Formats (`docs/protocol.md`)**](docs/protocol.md) — Binary layouts, QUIC `lore-storage/0.4` framing, gRPC services, and P2P tracker/chunk protocols.
- [**Configuration & Usage Guide (`docs/configuration-and-usage.md`)**](docs/configuration-and-usage.md) — Complete `default.toml` / `peer.toml` reference and CLI usage.
- [**Developer & Agent Guide (`AGENTS.md`)**](AGENTS.md) — Module map, wire invariants, and verification workflows.
