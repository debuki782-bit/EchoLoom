param(
    [switch]$Quiet
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$tempRoot = Join-Path $root '.tmp'

$python = $null
foreach ($candidate in @(
    $env:ECHOLOOM_PYTHON,
    (Join-Path $root '.venv\Scripts\python.exe')
)) {
    if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
        $python = $candidate
        break
    }
}

$dashboardPort = 8767
if ($python) {
    try {
        $runtimeMetaJson = & $python -m echoloom.local_config runtime-json 2>$null
        if ($LASTEXITCODE -eq 0 -and $runtimeMetaJson) {
            $runtimeMeta = $runtimeMetaJson | ConvertFrom-Json
            if ($runtimeMeta.server -and $runtimeMeta.server.dashboard_port) {
                $dashboardPort = [int]$runtimeMeta.server.dashboard_port
            }
        }
    } catch {}
}

function Test-CmdMatchesScript([string]$cmd, [string]$scriptPath) {
    if ([string]::IsNullOrEmpty($cmd) -or [string]::IsNullOrEmpty($scriptPath)) { return $false }
    $full = [IO.Path]::GetFullPath($scriptPath)
    $fwd = $full.Replace('\', '/')
    return (
        $cmd.IndexOf($full, [System.StringComparison]::OrdinalIgnoreCase) -ge 0 -or
        $cmd.IndexOf($fwd, [System.StringComparison]::OrdinalIgnoreCase) -ge 0
    )
}

function Get-ProjectPythonProcesses([string]$scriptPath) {
    @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.Name -in @('python.exe', 'pythonw.exe') -and
        $_.CommandLine -and
        (Test-CmdMatchesScript $_.CommandLine $scriptPath)
    })
}

function Stop-VerifiedProcesses($processes) {
    $ordered = @($processes | Sort-Object CreationDate -Descending)
    foreach ($proc in $ordered) {
        if (Get-Process -Id $proc.ProcessId -ErrorAction SilentlyContinue) {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
        }
    }
    foreach ($proc in $ordered) {
        for ($i = 0; $i -lt 20; $i++) {
            if (-not (Get-Process -Id $proc.ProcessId -ErrorAction SilentlyContinue)) { break }
            Start-Sleep -Milliseconds 250
        }
    }
}

function Get-ListeningPids([int]$port) {
    @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique)
}

$results = @()
$allOk = $true

function Add-Result([string]$name, [bool]$ok, [string]$detail) {
    $script:results += [pscustomobject]@{ Name = $name; Ok = $ok; Detail = $detail }
    if (-not $ok) { $script:allOk = $false }
}

# 1. Stop watch-services.py first so self-healing does not relaunch services mid-shutdown.
$watchScript = Join-Path $PSScriptRoot 'watch-services.py'
try {
    $watchProcs = Get-ProjectPythonProcesses $watchScript
    if ($watchProcs.Count -eq 0) {
        Add-Result 'Service Monitor' $true '未运行'
    } else {
        Stop-VerifiedProcesses $watchProcs
        $remaining = Get-ProjectPythonProcesses $watchScript
        if ($remaining.Count -eq 0) {
            Add-Result 'Service Monitor' $true ("已停止 (" + $watchProcs.Count + " 个进程)")
        } else {
            Add-Result 'Service Monitor' $false '部分监控进程未能停止'
        }
    }
} catch {
    Add-Result 'Service Monitor' $false $_.Exception.Message
}

# 2. Stop Agent Ingress + Worker (8080).
$liveScript = Join-Path $PSScriptRoot 'live.ps1'
try {
    $out = (& $liveScript -Action stop 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) { throw $out }
    Add-Result 'Agent Ingress+Worker (8080)' $true ($out -split "`r?`n" | Select-Object -Last 1)
} catch {
    Add-Result 'Agent Ingress+Worker (8080)' $false $_.Exception.Message
}

# 3. Stop Hook Event Extension (30003) gracefully before touching anything else.
$hookExtScript = Join-Path $PSScriptRoot 'hook-event-extension.ps1'
if (Test-Path -LiteralPath $hookExtScript -PathType Leaf) {
    try {
        $out = (& $hookExtScript -Action stop 2>&1 | Out-String).Trim()
        if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) { throw $out }
        Add-Result '长卡片/拍一拍扩展 (30003)' $true ($out -split "`r?`n" | Select-Object -Last 1)
    } catch {
        Add-Result '长卡片/拍一拍扩展 (30003)' $false $_.Exception.Message
    }
}

# 4. Stop Native Image Download (30002).
$nativeImgScript = Join-Path $PSScriptRoot 'native-image-download.ps1'
if (Test-Path -LiteralPath $nativeImgScript -PathType Leaf) {
    try {
        $out = (& $nativeImgScript -Action stop 2>&1 | Out-String).Trim()
        if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) { throw $out }
        Add-Result '原图下载扩展 (30002)' $true ($out -split "`r?`n" | Select-Object -Last 1)
    } catch {
        Add-Result '原图下载扩展 (30002)' $false $_.Exception.Message
    }
}

# 5. Stop Trace Dashboard (8767).
$dashboardScript = Join-Path $PSScriptRoot 'trace-dashboard.py'
$dashboardPidFile = Join-Path $tempRoot 'trace-dashboard.pid'
try {
    $listeners = Get-ListeningPids $dashboardPort
    $dashProcs = Get-ProjectPythonProcesses $dashboardScript
    $dashIds = @($dashProcs | Select-Object -ExpandProperty ProcessId)
    $foreign = @($listeners | Where-Object { $_ -notin $dashIds })
    if ($foreign.Count -gt 0) {
        Add-Result "Trace Dashboard ($dashboardPort)" $false ("端口 $dashboardPort 被非本项目进程占用 (PID: $($foreign -join ', '))，已跳过")
    } elseif ($dashProcs.Count -eq 0 -and $listeners.Count -eq 0) {
        if (Test-Path -LiteralPath $dashboardPidFile) { Remove-Item -LiteralPath $dashboardPidFile -Force -ErrorAction SilentlyContinue }
        Add-Result "Trace Dashboard ($dashboardPort)" $true '未运行'
    } else {
        Stop-VerifiedProcesses $dashProcs
        for ($i = 0; $i -lt 20; $i++) {
            if ((Get-ListeningPids $dashboardPort).Count -eq 0 -and (Get-ProjectPythonProcesses $dashboardScript).Count -eq 0) { break }
            Start-Sleep -Milliseconds 250
        }
        if ((Get-ListeningPids $dashboardPort).Count -eq 0 -and (Get-ProjectPythonProcesses $dashboardScript).Count -eq 0) {
            if (Test-Path -LiteralPath $dashboardPidFile) { Remove-Item -LiteralPath $dashboardPidFile -Force -ErrorAction SilentlyContinue }
            Add-Result "Trace Dashboard ($dashboardPort)" $true '已停止'
        } else {
            Add-Result "Trace Dashboard ($dashboardPort)" $false "端口 $dashboardPort 仍未释放"
        }
    }
} catch {
    Add-Result "Trace Dashboard ($dashboardPort)" $false $_.Exception.Message
}

if (-not $Quiet) {
    Write-Host ''
    Write-Host '================ 服务停止汇总 ================'
    foreach ($item in $results) {
        $tag = if ($item.Ok) { '[ OK ]' } else { '[FAIL]' }
        $color = if ($item.Ok) { 'Green' } else { 'Red' }
        Write-Host ("  {0} {1,-28} | {2}" -f $tag, $item.Name, $item.Detail) -ForegroundColor $color
    }
    Write-Host '  [KEEP] 微信本体 & Hook (30001)     | 已保留未关闭' -ForegroundColor DarkCyan
    Write-Host '=============================================='
}

if (-not $allOk) { exit 1 }
exit 0
