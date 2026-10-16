# Configuration & Usage Guide

Bitlore builds two standalone binaries from this repository:

1. **`bitlore-server`**: The metadata-only Lore server and P2P swarm tracker.
2. **`bitlore-peer`**: The local P2P workstation sidecar that caches file chunks locally, announces owned files to the tracker, and transfers chunks peer-to-peer over `iroh` QUIC.

---

## 1. Building from Source

```powershell
# Build both bitlore-server and bitlore-peer
cargo build --bins

# Build optimized release binaries
cargo build --release --bins

# Run unit tests
cargo test
```

---

## 2. Running `bitlore-server`

```powershell
# Run using config/default.toml (listens on TCP/UDP 42337, P2P Tracker on TCP 42340)
.\target\debug\bitlore-server.exe --config config
```

### `bitlore-server` Configuration (`config/default.toml`)

```toml
[server.grpc]
port = 42337

[server.quic]
port = 42337

[immutable_store]
mode = "bitlore"

[plugins.bitlore.immutable_store]
# Directory for structural fragments (State, Tree, NodeBlock, NameTable, Metadata).
# Leave empty to use <temp_dir>/bitlore-server/immutable.
path = "./data/server-immutable"
flush_delay_seconds = 10
# When false (default), only structural fragments (context == 0) are stored on disk.
store_file_payloads = false
# Unary gRPC listener for the Bitlore P2P Tracker (/bitlore.p2p.v1.P2pTracker).
tracker_bind_addr = "0.0.0.0:42340"
peer_ttl_seconds = 120

[mutable_store]
mode = "local"

[mutable_store.local]
path = "./data/server-mutable"

# Optional: When teammates run bitlore-peer locally on port 41399, set storage_url
# so vanilla `lore` clients automatically route QUIC fragment reads/writes to their
# local P2P sidecar while keeping repository/branch/revision calls on bitlore-server:
#
# [environment.endpoint]
# api_url = "lore://127.0.0.1:42337"
# storage_url = "lore://127.0.0.1:41399"
```

---

## 3. Running `bitlore-peer` (Workstation P2P Sidecar)

On each developer workstation, run `bitlore-peer` with a peer configuration file:

```powershell
.\target\debug\bitlore-peer.exe --config peer.toml
```

### `bitlore-peer` Configuration (`peer.toml`)

```toml
[server.grpc]
port = 41399

[server.quic]
port = 41399

[immutable_store]
mode = "bitlore-peer"

[plugins.bitlore_peer.immutable_store]
# Local content-addressed cache for file chunks and structural fragments.
path = "./data/peer-immutable"
# Address of the P2P tracker on bitlore-server.
tracker_url = "http://127.0.0.1:42340"
# Optional UDP port for iroh P2P QUIC connections (0 = OS-assigned port).
p2p_bind_port = 0
# Enable iroh relay fallback alongside direct UDP hole-punching.
enable_relay = true
# Interval between tracker liveness heartbeats.
heartbeat_interval_seconds = 25

[mutable_store]
mode = "local"

[mutable_store.local]
path = "./data/peer-mutable"
```

---

## 4. Using Vanilla `lore` Clients with Bitlore

With `bitlore-server` and `bitlore-peer` running, team members use standard `lore` commands with no special flags:

```powershell
# Create a repository on bitlore-server
lore repository create lore://127.0.0.1:42337/my-game-repo ./worktree-a

# Stage, commit, and push binary assets as usual
cd ./worktree-a
lore stage .
lore commit -m "Add level assets"
lore branch push

# Another teammate clones the repository — metadata comes from bitlore-server,
# while binary file chunks stream over iroh P2P from active peers!
lore clone lore://127.0.0.1:42337/my-game-repo ./worktree-b
```

---

## 5. End-to-End Verification Scripts

Bitlore includes two automated PowerShell E2E verification suites in [`scripts/`](../scripts) that test against an unmodified vanilla `lore.exe` binary (resolved via `$env:LORE_EXE` or `../lore/target/debug/lore.exe`):

1. **Metadata-Only Hosting E2E ([`scripts/e2e_verify.ps1`](../scripts/e2e_verify.ps1))**:
   ```powershell
   pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/e2e_verify.ps1
   ```
   Verifies repository creation, online commit (5 MiB binary), `--local` commit (3 MiB binary), branch push, bare clone (`--bare`), full clone with shared store, incremental `lore sync`, SHA-256 integrity, and confirms `bitlore-server` stores only ~4 KiB of structural metadata for 8 MiB of committed assets.

2. **Full P2P Swarm + `iroh` E2E ([`scripts/e2e_p2p_verify.ps1`](../scripts/e2e_p2p_verify.ps1))**:
   ```powershell
   pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/e2e_p2p_verify.ps1
   ```
   Spins up `bitlore-server` and three isolated workstation peers (`Peer A`, `Peer B`, `Peer C`), pushes 8 MiB across 2 commits from Teammate A, clones from scratch on Teammate B over `iroh` P2P, kills `Peer A` to test swarm resilience, and verifies Teammate C clones all 8 MiB from `Peer B` with 100% matching SHA-256 hashes.
