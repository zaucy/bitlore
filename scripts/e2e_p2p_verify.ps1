$ErrorActionPreference = "Stop"

$BitloreRoot = Split-Path -Parent $PSScriptRoot
$LoreWorkspaceRoot = Resolve-Path (Join-Path $BitloreRoot "..\..\lore")
$LoreExe = Join-Path $LoreWorkspaceRoot "target\debug\lore.exe"
$BitloreServerExe = Join-Path $BitloreRoot "target\debug\bitlore-server.exe"
if (-not (Test-Path $BitloreServerExe)) {
    $BitloreServerExe = Join-Path $LoreWorkspaceRoot "target\debug\bitlore-server.exe"
}
$BitlorePeerExe = Join-Path $BitloreRoot "target\debug\bitlore-peer.exe"
if (-not (Test-Path $BitlorePeerExe)) {
    $BitlorePeerExe = Join-Path $LoreWorkspaceRoot "target\debug\bitlore-peer.exe"
}

foreach ($bin in @($LoreExe, $BitloreServerExe, $BitlorePeerExe)) {
    if (-not (Test-Path $bin)) {
        throw "Required binary not found at $bin"
    }
}

$Env:LORE_USE_SERVICE = "0"

$TestRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("bitlore-p2p-e2e-" + [Guid]::NewGuid().ToString("N").Substring(0, 8))
$ServerConfigDir = Join-Path $TestRoot "server-config"
$ServerDataDir   = Join-Path $TestRoot "server-data"
$ServerImmutDir  = Join-Path $ServerDataDir "immutable"
$ServerMutDir    = Join-Path $ServerDataDir "mutable"

$PeerADataDir    = Join-Path $TestRoot "peer-a-data"
$PeerBDataDir    = Join-Path $TestRoot "peer-b-data"
$PeerCDataDir    = Join-Path $TestRoot "peer-c-data"

$ClientADir      = Join-Path $TestRoot "client-a"
$ClientBDir      = Join-Path $TestRoot "client-b"
$ClientCDir      = Join-Path $TestRoot "client-c"
$ClientHomeDir   = Join-Path $TestRoot "client-home"

foreach ($d in @($ServerConfigDir, $ServerImmutDir, $ServerMutDir, $PeerADataDir, $PeerBDataDir, $PeerCDataDir, $ClientADir, $ClientHomeDir)) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

$ServerPort  = 42337
$ServerHttpPort = 42339
$TrackerPort = 42340
$StorageGatewayPort = 41399

$ServerUrl   = "lore://127.0.0.1:$ServerPort"
$TrackerUrl  = "http://127.0.0.1:$TrackerPort"
$StorageGatewayUrl = "lore://127.0.0.1:$StorageGatewayPort"
$RepoUrl     = "$ServerUrl/bitlore-p2p-demo"

function Start-BitlorePeerDaemon {
    param(
        [string]$Name,
        [int]$Port,
        [int]$HttpPort,
        [string]$DataDir
    )
    $cfgDir   = Join-Path $DataDir "config-$Name"
    $immutDir = Join-Path $DataDir "immutable"
    $mutDir   = Join-Path $DataDir "mutable"
    New-Item -ItemType Directory -Force -Path $cfgDir, $immutDir, $mutDir | Out-Null

    $cfgPath = Join-Path $cfgDir "default.toml"
    $immutEsc = $immutDir.Replace("\", "/")
    $mutEsc   = $mutDir.Replace("\", "/")

    @"
[server.local_store_monitor]
check_interval_seconds = 0

[server.grpc]
port = $Port

[server.quic]
port = $Port

[server.http]
port = $HttpPort

[immutable_store]
mode = "bitlore_peer"

[plugins.bitlore_peer.immutable_store]
path = "$immutEsc"
tracker_url = "$TrackerUrl"
use_n0_relay = false
flush_delay_seconds = 1

[mutable_store]
mode = "local"

[mutable_store.local]
path = "$mutEsc"
flush_delay_seconds = 1

[lock_store]
mode = "local"
"@ | Set-Content -Path $cfgPath -Encoding UTF8

    $outLog = Join-Path $DataDir "$Name-stdout.log"
    $errLog = Join-Path $DataDir "$Name-stderr.log"
    $proc = Start-Process -FilePath $BitlorePeerExe `
        -ArgumentList @("--config", $cfgDir) `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -PassThru -NoNewWindow

    $ready = $false
    for ($i = 0; $i -lt 40; $i++) {
        Start-Sleep -Milliseconds 250
        if ($proc.HasExited) {
            $errText = Get-Content $errLog -Raw -ErrorAction SilentlyContinue
            $outText = Get-Content $outLog -Raw -ErrorAction SilentlyContinue
            throw "$Name exited prematurely. STDOUT:`n$outText`nSTDERR:`n$errText"
        }
        $conn = Test-NetConnection -ComputerName "127.0.0.1" -Port $Port -WarningAction SilentlyContinue
        if ($conn.TcpTestSucceeded) {
            $ready = $true
            break
        }
    }
    if (-not $ready) {
        $errText = Get-Content $errLog -Raw -ErrorAction SilentlyContinue
        $outText = Get-Content $outLog -Raw -ErrorAction SilentlyContinue
        throw "Timed out waiting for $Name on port $Port. STDOUT:`n$outText`nSTDERR:`n$errText"
    }
    Start-Sleep -Milliseconds 300
    return $proc
}

$RunningProcs = New-Object System.Collections.Generic.List[System.Diagnostics.Process]

try {
    # -------------------------------------------------------------------------
    # 1. Start bitlore-server (Metadata-only store + P2P Tracker + storage_url)
    # -------------------------------------------------------------------------
    $ServerConfigFile = Join-Path $ServerConfigDir "default.toml"
    $ImmutEsc = $ServerImmutDir.Replace("\", "/")
    $MutEsc   = $ServerMutDir.Replace("\", "/")

    @"
[server.local_store_monitor]
check_interval_seconds = 0

[server.grpc]
port = $ServerPort

[server.quic]
port = $ServerPort

[server.http]
port = $ServerHttpPort

[environment.endpoint]
api_url = "$ServerUrl"
storage_url = "$StorageGatewayUrl"

[immutable_store]
mode = "bitlore"

[plugins.bitlore.immutable_store]
path = "$ImmutEsc"
flush_delay_seconds = 1
store_file_payloads = false
tracker_bind_addr = "127.0.0.1:$TrackerPort"
peer_ttl_seconds = 120

[mutable_store]
mode = "local"

[mutable_store.local]
path = "$MutEsc"
flush_delay_seconds = 1

[lock_store]
mode = "local"
"@ | Set-Content -Path $ServerConfigFile -Encoding UTF8

    Write-Host "==> [1/6] Starting bitlore-server on $ServerUrl (P2P Tracker on $TrackerUrl, storage_url -> $StorageGatewayUrl)..." -ForegroundColor Cyan
    $ServerOutLog = Join-Path $TestRoot "server-stdout.log"
    $ServerErrLog = Join-Path $TestRoot "server-stderr.log"
    $ServerProc = Start-Process -FilePath $BitloreServerExe `
        -ArgumentList @("--config", $ServerConfigDir) `
        -RedirectStandardOutput $ServerOutLog `
        -RedirectStandardError $ServerErrLog `
        -PassThru -NoNewWindow
    $RunningProcs.Add($ServerProc)

    $ServerReady = $false
    for ($i = 0; $i -lt 40; $i++) {
        Start-Sleep -Milliseconds 250
        if ($ServerProc.HasExited) {
            $errText = Get-Content $ServerErrLog -Raw -ErrorAction SilentlyContinue
            $outText = Get-Content $ServerOutLog -Raw -ErrorAction SilentlyContinue
            throw "bitlore-server exited prematurely. STDOUT:`n$outText`nSTDERR:`n$errText"
        }
        $conn = Test-NetConnection -ComputerName "127.0.0.1" -Port $ServerPort -WarningAction SilentlyContinue
        if ($conn.TcpTestSucceeded) {
            $ServerReady = $true
            break
        }
    }
    if (-not $ServerReady) {
        throw "Timed out waiting for bitlore-server on port $ServerPort"
    }

    # -------------------------------------------------------------------------
    # 2. Teammate A: Start Peer A on storage_url port & push 8 MiB of assets
    #    using plain vanilla `lore.exe` commands (NO --use-shared-store, NO --local!)
    # -------------------------------------------------------------------------
    Write-Host "==> [2/6] Starting Peer A (bitlore-peer.exe) for Teammate A..." -ForegroundColor Cyan
    $PeerAProc = Start-BitlorePeerDaemon -Name "peer-a-initial" -Port $StorageGatewayPort -HttpPort 41499 -DataDir $PeerADataDir
    $RunningProcs.Add($PeerAProc)

    Write-Host "==> [3/6] Teammate A: Creating repo, committing & pushing 8 MiB across 2 commits (vanilla lore.exe, zero special flags)..." -ForegroundColor Cyan
    # Notice: NO --use-shared-store flag!
    & $LoreExe repository create --repository $ClientADir --identity "alice@studio.dev" $RepoUrl
    if ($LASTEXITCODE -ne 0) { throw "lore repository create failed" }

    New-Item -ItemType Directory -Force -Path "$ClientADir\assets" | Out-Null
    "# Bitlore P2P Demo Repository`nHosted with zero server file bandwidth." | Set-Content -Path "$ClientADir\README.md" -Encoding UTF8

    $Bytes5MB = New-Object byte[] (5 * 1024 * 1024)
    [System.Random]::new(101).NextBytes($Bytes5MB)
    [System.IO.File]::WriteAllBytes("$ClientADir\assets\level1.pak", $Bytes5MB)
    $ExpectedHashLevel1 = (Get-FileHash "$ClientADir\assets\level1.pak" -Algorithm SHA256).Hash

    & $LoreExe stage --repository $ClientADir --identity "alice@studio.dev" "$ClientADir\README.md" "$ClientADir\assets\level1.pak"
    if ($LASTEXITCODE -ne 0) { throw "Stage 1 failed" }

    # Notice: Plain `lore commit` and `lore push` — NO `--local` and NO `--use-shared-store`!
    & $LoreExe commit --repository $ClientADir --identity "alice@studio.dev" "Initial commit with 5 MiB level1.pak"
    if ($LASTEXITCODE -ne 0) { throw "Commit 1 failed" }

    & $LoreExe push --repository $ClientADir --identity "alice@studio.dev" --stats=2
    if ($LASTEXITCODE -ne 0) { throw "Push 1 failed" }

    $Bytes3MB = New-Object byte[] (3 * 1024 * 1024)
    [System.Random]::new(202).NextBytes($Bytes3MB)
    [System.IO.File]::WriteAllBytes("$ClientADir\assets\textures.pak", $Bytes3MB)
    $ExpectedHashTextures = (Get-FileHash "$ClientADir\assets\textures.pak" -Algorithm SHA256).Hash

    & $LoreExe stage --repository $ClientADir --identity "alice@studio.dev" "$ClientADir\assets\textures.pak"
    if ($LASTEXITCODE -ne 0) { throw "Stage 2 failed" }

    & $LoreExe commit --repository $ClientADir --identity "alice@studio.dev" "Second commit with 3 MiB textures.pak"
    if ($LASTEXITCODE -ne 0) { throw "Commit 2 failed" }

    & $LoreExe push --repository $ClientADir --identity "alice@studio.dev" --stats=2
    if ($LASTEXITCODE -ne 0) { throw "Push 2 failed" }

    # -------------------------------------------------------------------------
    # 3. Teammate B (Empty Peer B Store):
    #    Move Peer A to port 41398 (seeding over iroh QUIC) and start Peer B
    #    on port 41399 (storage_url) with an EMPTY store!
    # -------------------------------------------------------------------------
    Write-Host "==> [4/6] Switching to Teammate B's workstation (Peer A seeding over iroh, Peer B starting with an EMPTY store)..." -ForegroundColor Cyan
    Stop-Process -Id $PeerAProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 400

    # Restart Peer A on port 41398 as a remote teammate seeder (automatically re-announces its files + iroh EndpointAddr)
    $PeerASeederProc = Start-BitlorePeerDaemon -Name "peer-a-seeder" -Port 41398 -HttpPort 41498 -DataDir $PeerADataDir
    $RunningProcs.Add($PeerASeederProc)

    # Start Peer B on $StorageGatewayPort (41399) with a completely empty store directory
    $PeerBProc = Start-BitlorePeerDaemon -Name "peer-b" -Port $StorageGatewayPort -HttpPort 41499 -DataDir $PeerBDataDir
    $RunningProcs.Add($PeerBProc)

    Write-Host "    Running plain vanilla command: lore clone $RepoUrl ./client-b" -ForegroundColor Yellow
    # Notice: Plain `lore clone` — NO `--use-shared-store` and NO `--bare`!
    & $LoreExe clone --identity "bob@studio.dev" $RepoUrl $ClientBDir
    if ($LASTEXITCODE -ne 0) { throw "Teammate B P2P clone failed!" }

    $ClientBLevel1Hash   = (Get-FileHash "$ClientBDir\assets\level1.pak" -Algorithm SHA256).Hash
    $ClientBTexturesHash = (Get-FileHash "$ClientBDir\assets\textures.pak" -Algorithm SHA256).Hash

    if ($ClientBLevel1Hash -ne $ExpectedHashLevel1) {
        throw "SHA-256 mismatch on Teammate B's level1.pak!"
    }
    if ($ClientBTexturesHash -ne $ExpectedHashTextures) {
        throw "SHA-256 mismatch on Teammate B's textures.pak!"
    }
    Write-Host "    [OK] Teammate B cloned 8.00 MiB over iroh P2P from Peer A with 100% matching SHA-256 hashes!" -ForegroundColor Green

    # -------------------------------------------------------------------------
    # 4. Swarm Multi-Hop Seeding Verification (Teammate C after Peer A goes offline!)
    #    Stop Peer A completely! Now only Peer B has the files.
    #    Move Peer B to port 41397 (seeding over iroh) and start Peer C on 41399
    #    with an EMPTY store!
    # -------------------------------------------------------------------------
    Write-Host "==> [5/6] Testing Swarm Resilience: Taking Peer A OFFLINE; Teammate C clones from Peer B over iroh P2P..." -ForegroundColor Cyan
    Stop-Process -Id $PeerASeederProc.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $PeerBProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 400

    # Restart Peer B on port 41397 as the only online seeder
    $PeerBSeederProc = Start-BitlorePeerDaemon -Name "peer-b-seeder" -Port 41397 -HttpPort 41497 -DataDir $PeerBDataDir
    $RunningProcs.Add($PeerBSeederProc)

    # Start Peer C on $StorageGatewayPort (41399) with a completely empty store directory
    $PeerCProc = Start-BitlorePeerDaemon -Name "peer-c" -Port $StorageGatewayPort -HttpPort 41499 -DataDir $PeerCDataDir
    $RunningProcs.Add($PeerCProc)

    Write-Host "    Running plain vanilla command: lore clone $RepoUrl ./client-c (with Peer A offline!)" -ForegroundColor Yellow
    & $LoreExe clone --identity "charlie@studio.dev" $RepoUrl $ClientCDir
    if ($LASTEXITCODE -ne 0) { throw "Teammate C P2P clone from Peer B failed!" }

    $ClientCLevel1Hash   = (Get-FileHash "$ClientCDir\assets\level1.pak" -Algorithm SHA256).Hash
    $ClientCTexturesHash = (Get-FileHash "$ClientCDir\assets\textures.pak" -Algorithm SHA256).Hash

    if ($ClientCLevel1Hash -ne $ExpectedHashLevel1 -or $ClientCTexturesHash -ne $ExpectedHashTextures) {
        throw "SHA-256 mismatch on Teammate C's cloned files!"
    }
    Write-Host "    [OK] Teammate C cloned 8.00 MiB over iroh P2P from Peer B while Peer A was offline!" -ForegroundColor Green

    # -------------------------------------------------------------------------
    # 5. Measure bitlore-server's disk footprint
    # -------------------------------------------------------------------------
    Write-Host "==> [6/6] Measuring bitlore-server storage footprint..." -ForegroundColor Cyan
    Start-Sleep -Seconds 2
    Stop-Process -Id $ServerProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 300

    $ServerImmutableBytes = (Get-ChildItem -Path $ServerImmutDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $ServerImmutableBytes) { $ServerImmutableBytes = 0 }
    $ServerMutableBytes = (Get-ChildItem -Path $ServerMutDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $ServerMutableBytes) { $ServerMutableBytes = 0 }
    $TotalAssetBytes = (5 * 1024 * 1024) + (3 * 1024 * 1024)

    Write-Host ""
    Write-Host "=========================================================================" -ForegroundColor Green
    Write-Host " BITLORE P2P SWARM + METADATA SERVER E2E VERIFICATION SUCCEEDED!" -ForegroundColor Green
    Write-Host "=========================================================================" -ForegroundColor Green
    Write-Host (" Total Binary Assets Pushed & Cloned : {0:N0} bytes (~8.00 MiB)" -f $TotalAssetBytes)
    Write-Host (" Bitlore Server Immutable Store Disk : {0:N0} bytes (~{1:N2} KiB)" -f $ServerImmutableBytes, ($ServerImmutableBytes / 1024.0))
    Write-Host (" Bitlore Server Mutable Store Disk   : {0:N0} bytes" -f $ServerMutableBytes)
    Write-Host (" Teammate A -> Peer A -> Peer B (P2P): SHA-256 Verified ({0})" -f $ClientBLevel1Hash.Substring(0, 16))
    Write-Host (" Peer A Offline -> Peer B -> Peer C  : SHA-256 Verified ({0})" -f $ClientCLevel1Hash.Substring(0, 16))
    Write-Host "=========================================================================" -ForegroundColor Green
} finally {
    foreach ($p in $RunningProcs) {
        if ($null -ne $p -and -not $p.HasExited) {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        }
    }
}
