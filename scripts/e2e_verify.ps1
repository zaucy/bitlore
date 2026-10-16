$ErrorActionPreference = "Stop"

$BitloreRoot = Split-Path -Parent $PSScriptRoot
$LoreWorkspaceRoot = Resolve-Path (Join-Path $BitloreRoot "..\..\lore")
$BitloreServerExe = Join-Path $BitloreRoot "target\debug\bitlore-server.exe"
if (-not (Test-Path $BitloreServerExe)) {
    $BitloreServerExe = Join-Path $LoreWorkspaceRoot "target\debug\bitlore-server.exe"
}
$LoreCliExe = Join-Path $LoreWorkspaceRoot "target\debug\lore.exe"

if (-not (Test-Path $BitloreServerExe)) {
    throw "bitlore-server.exe not found at $BitloreServerExe. Run 'cargo build --bin bitlore-server' in bitlore/ first."
}
if (-not (Test-Path $LoreCliExe)) {
    throw "lore.exe not found at $LoreCliExe. Run 'cargo build --bin lore' in the lore workspace first."
}

$TestRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("bitlore-e2e-" + [guid]::NewGuid().ToString("N").Substring(0, 8))
New-Item -ItemType Directory -Force -Path $TestRoot | Out-Null

$ServerConfigDir = "$TestRoot\config"
$ServerImmutableDir = ($TestRoot + "\server-immutable").Replace("\", "/")
$ServerMutableDir = ($TestRoot + "\server-mutable").Replace("\", "/")
$SharedP2PStoreDir = "$TestRoot\p2p-shared-store"
$ClientADir = "$TestRoot\client-a"
$ClientBBareDir = "$TestRoot\client-b-bare"
$ClientBFullDir = "$TestRoot\client-b-full"
$ServerLog = "$TestRoot\bitlore-server.log"
$ServerErrLog = "$TestRoot\bitlore-server.err.log"

New-Item -ItemType Directory -Force -Path $ServerConfigDir, $ServerImmutableDir, $ServerMutableDir, $SharedP2PStoreDir, $ClientADir | Out-Null

$GrpcPort = 42337
$HttpPort = 42339

$TomlContent = @"
[server.local_store_monitor]
check_interval_seconds = 0

[server.grpc]
port = $GrpcPort

[server.quic]
port = $GrpcPort

[server.http]
port = $HttpPort

[immutable_store]
mode = "bitlore"

[plugins.bitlore.immutable_store]
path = "$ServerImmutableDir"
flush_delay_seconds = 1
store_file_payloads = false

[mutable_store]
mode = "local"

[mutable_store.local]
path = "$ServerMutableDir"
flush_delay_seconds = 1

[lock_store]
mode = "local"
"@

Set-Content -Path "$ServerConfigDir\default.toml" -Value $TomlContent

$Env:LORE_USE_SERVICE = "0"

Write-Host "=== 1. Starting bitlore-server on port $GrpcPort ==="
$ServerProc = Start-Process -FilePath $BitloreServerExe -ArgumentList "--config", $ServerConfigDir -RedirectStandardOutput $ServerLog -RedirectStandardError $ServerErrLog -PassThru -NoNewWindow

try {
    $Ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Milliseconds 500
        if ($ServerProc.HasExited) {
            Get-Content $ServerLog -ErrorAction SilentlyContinue
            Get-Content $ServerErrLog -ErrorAction SilentlyContinue
            throw "bitlore-server exited prematurely"
        }
        $Conn = Test-NetConnection -ComputerName 127.0.0.1 -Port $GrpcPort -WarningAction SilentlyContinue
        if ($Conn.TcpTestSucceeded) {
            $Ready = $true
            break
        }
    }
    if (-not $Ready) {
        throw "Timed out waiting for bitlore-server on port $GrpcPort"
    }
    Write-Host "bitlore-server is up and listening!"

    $RepoUrl = "lore://127.0.0.1:$GrpcPort/bitlore-demo"

    Write-Host "`n=== 2. [Vanilla lore.exe - Client A] Creating repository $RepoUrl ==="
    & $LoreCliExe repository create --repository $ClientADir --use-shared-store --shared-store-path $SharedP2PStoreDir --identity "alice@studio.dev" $RepoUrl
    if ($LASTEXITCODE -ne 0) { throw "repository create failed" }

    Write-Host "`n=== 3. [Vanilla lore.exe - Client A] Creating source files + 5 MiB binary asset ==="
    New-Item -ItemType Directory -Force -Path "$ClientADir\src", "$ClientADir\assets" | Out-Null
    Set-Content -Path "$ClientADir\README.md" -Value "# Bitlore Studio Repo`nHosted on Bitlore metadata-only server!"
    Set-Content -Path "$ClientADir\src\main.rs" -Value "fn main() { println!(`"Hello from Bitlore!`"); }"

    $AssetPath = "$ClientADir\assets\level1.pak"
    $Bytes = New-Object byte[] (5 * 1024 * 1024)
    $Rng = [System.Random]::new(1337)
    $Rng.NextBytes($Bytes)
    [System.IO.File]::WriteAllBytes($AssetPath, $Bytes)
    $OriginalHash = (Get-FileHash -Path $AssetPath -Algorithm SHA256).Hash
    Write-Host "Created 5 MiB binary asset ($($Bytes.Length) bytes), SHA256: $OriginalHash"

    Write-Host "`n=== 4. [Vanilla lore.exe - Client A] Staging and Committing (default online commit) ==="
    & $LoreCliExe stage --repository $ClientADir --identity "alice@studio.dev" "$ClientADir\README.md" "$ClientADir\src\main.rs" "$ClientADir\assets\level1.pak"
    if ($LASTEXITCODE -ne 0) { throw "lore stage failed" }

    & $LoreCliExe commit --repository $ClientADir --identity "alice@studio.dev" "Initial commit with 5 MiB asset and source files"
    if ($LASTEXITCODE -ne 0) { throw "lore commit failed" }

    Write-Host "`n=== 5. [Vanilla lore.exe - Client A] Pushing to bitlore-server (with --stats=2) ==="
    & $LoreCliExe push --repository $ClientADir --identity "alice@studio.dev" --stats=2
    if ($LASTEXITCODE -ne 0) { throw "lore push failed" }

    Write-Host "`n=== 6. [Vanilla lore.exe - Client B] Bare Clone directly from bitlore-server (NO shared store!) ==="
    & $LoreCliExe clone --bare --identity "bob@studio.dev" $RepoUrl $ClientBBareDir
    if ($LASTEXITCODE -ne 0) { throw "lore clone --bare failed" }

    Write-Host "`n=== 7. [Vanilla lore.exe - Client B Bare] Inspecting history on bare clone ==="
    & $LoreCliExe history --repository $ClientBBareDir --identity "bob@studio.dev"
    if ($LASTEXITCODE -ne 0) { throw "lore history failed" }

    Write-Host "`n=== 8. [Vanilla lore.exe - Client B] Full Clone using bitlore-server metadata + P2P/shared store ==="
    & $LoreCliExe clone --use-shared-store --shared-store-path $SharedP2PStoreDir --identity "bob@studio.dev" $RepoUrl $ClientBFullDir
    if ($LASTEXITCODE -ne 0) { throw "lore clone full with shared store failed" }

    $ClonedAssetPath = "$ClientBFullDir\assets\level1.pak"
    $ClonedHash = (Get-FileHash -Path $ClonedAssetPath -Algorithm SHA256).Hash
    if ($ClonedHash -ne $OriginalHash) {
        throw "SHA256 mismatch on cloned 5 MiB asset!"
    }
    Write-Host "Verified 5 MiB asset in Client B matches Client A byte-for-byte ($ClonedHash)!"

    Write-Host "`n=== 9. [Vanilla lore.exe - Client A] Commit #2 with --local (zero file bytes over wire) + Push ==="
    $Asset2Path = "$ClientADir\assets\textures.pak"
    $Bytes2 = New-Object byte[] (3 * 1024 * 1024)
    $Rng.NextBytes($Bytes2)
    [System.IO.File]::WriteAllBytes($Asset2Path, $Bytes2)
    $Asset2Hash = (Get-FileHash -Path $Asset2Path -Algorithm SHA256).Hash

    & $LoreCliExe stage --repository $ClientADir --identity "alice@studio.dev" "$Asset2Path"
    if ($LASTEXITCODE -ne 0) { throw "lore stage #2 failed" }

    & $LoreCliExe commit --local --repository $ClientADir --identity "alice@studio.dev" "Add 3 MiB textures.pak using --local"
    if ($LASTEXITCODE -ne 0) { throw "lore commit --local failed" }

    & $LoreCliExe push --repository $ClientADir --identity "alice@studio.dev" --stats=2
    if ($LASTEXITCODE -ne 0) { throw "lore push #2 failed" }

    Write-Host "`n=== 10. [Vanilla lore.exe - Client B] Syncing Commit #2 into Client B Full Clone ==="
    & $LoreCliExe sync --repository $ClientBFullDir --identity "bob@studio.dev"
    if ($LASTEXITCODE -ne 0) { throw "lore sync failed" }

    $PulledAsset2Hash = (Get-FileHash -Path "$ClientBFullDir\assets\textures.pak" -Algorithm SHA256).Hash
    if ($PulledAsset2Hash -ne $Asset2Hash) {
        throw "SHA256 mismatch on pulled 3 MiB textures.pak!"
    }
    Write-Host "Verified 3 MiB textures.pak synced by Client B matches Client A byte-for-byte ($PulledAsset2Hash)!"

    Start-Sleep -Seconds 2
    if (-not $ServerProc.HasExited) {
        Stop-Process -Id $ServerProc.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 500
    }

    $ServerImmutableBytes = (Get-ChildItem -Path $ServerImmutableDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $ServerImmutableBytes) { $ServerImmutableBytes = 0 }
    $ServerMutableBytes = (Get-ChildItem -Path $ServerMutableDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $ServerMutableBytes) { $ServerMutableBytes = 0 }
    $SharedStoreBytes = (Get-ChildItem -Path $SharedP2PStoreDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $SharedStoreBytes) { $SharedStoreBytes = 0 }

    $TotalWorkingTreeBytes = $Bytes.Length + $Bytes2.Length
    Write-Host "`n=== 11. On-Disk Storage Comparison ==="
    Write-Host ("Total working tree binary assets committed: {0:N0} bytes (~8.00 MiB)" -f $TotalWorkingTreeBytes)
    Write-Host ("P2P / Shared Store total on-disk bytes:     {0:N0} bytes" -f $SharedStoreBytes)
    Write-Host ("Bitlore Server immutable store on-disk:     {0:N0} bytes (Merkle tree & State metadata ONLY!)" -f $ServerImmutableBytes)
    Write-Host ("Bitlore Server mutable store on-disk:       {0:N0} bytes (Repo, branch & revision records)" -f $ServerMutableBytes)
}
finally {
    if (-not $ServerProc.HasExited) {
        Stop-Process -Id $ServerProc.Id -Force -ErrorAction SilentlyContinue
    }
}
