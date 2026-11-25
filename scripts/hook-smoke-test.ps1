param(
    [switch]$RequireHook,
    [string]$WeixinPath
)

$ErrorActionPreference = 'Stop'
$expectedVersion = '4.1.10.27'

function Resolve-WeixinPath {
    param([string]$OverridePath)

    if ($OverridePath) {
        if (Test-Path -LiteralPath $OverridePath -PathType Leaf) {
            return (Resolve-Path -LiteralPath $OverridePath).Path
        }
        throw "Weixin.exe path does not exist: $OverridePath"
    }

    # Prefer the executable actually running on this desktop. WeChat commonly
    # lives outside Program Files (for example on a secondary/data drive).
    $runningPaths = @(
        Get-CimInstance Win32_Process -Filter "Name='Weixin.exe'" -ErrorAction SilentlyContinue |
            Where-Object { $_.ExecutablePath -and (Test-Path -LiteralPath $_.ExecutablePath -PathType Leaf) } |
            Select-Object -ExpandProperty ExecutablePath -Unique
    )
    if ($runningPaths.Count -eq 1) { return $runningPaths[0] }
    if ($runningPaths.Count -gt 1) {
        throw ('Multiple running Weixin.exe paths found: ' + ($runningPaths -join '; '))
    }

    # If WeChat is closed, resolve the registered application path first.
    $registeredPaths = @(
        'HKCU:\Software\Microsoft\Windows\CurrentVersion\App Paths\Weixin.exe',
        'HKLM:\Software\Microsoft\Windows\CurrentVersion\App Paths\Weixin.exe',
        'HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\App Paths\Weixin.exe'
    ) | ForEach-Object {
        try {
            $value = (Get-Item -LiteralPath $_ -ErrorAction Stop).GetValue('')
            if ($value -and (Test-Path -LiteralPath $value -PathType Leaf)) { $value }
        } catch { }
    } | Select-Object -Unique
    if ($registeredPaths.Count -eq 1) { return $registeredPaths[0] }
    if ($registeredPaths.Count -gt 1) {
        throw ('Multiple registered Weixin.exe paths found: ' + ($registeredPaths -join '; '))
    }

    $commonPaths = @(
        'C:\Program Files\Tencent\Weixin\Weixin.exe',
        'C:\Program Files (x86)\Tencent\Weixin\Weixin.exe'
    ) | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf }
    if ($commonPaths.Count -eq 1) { return $commonPaths[0] }
    if ($commonPaths.Count -gt 1) {
        throw ('Multiple installed Weixin.exe paths found: ' + ($commonPaths -join '; '))
    }
    throw 'Weixin.exe not found in running processes, registered app paths, or standard install paths'
}

function Get-Sha12([string]$Value) {
    if ([string]::IsNullOrEmpty($Value)) { return 'missing' }
    $bytes = [Text.Encoding]::UTF8.GetBytes($Value)
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha.ComputeHash($bytes)
    } finally {
        $sha.Dispose()
    }
    return [BitConverter]::ToString($hash).Replace('-', '').Substring(0, 12).ToLowerInvariant()
}

$exe = Resolve-WeixinPath $WeixinPath
$version = (Get-Item -LiteralPath $exe).VersionInfo.ProductVersion
$signature = (Get-AuthenticodeSignature -LiteralPath $exe).Status.ToString()
$hookListener = @(Get-NetTCPConnection -LocalPort 30001 -State Listen -ErrorAction SilentlyContinue)
$receiverListener = @(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)
$hookReachable = $false
$receiverReachable = $false
$hookStatus = $null
$receiverStatus = $null

try {
    $hookStatus = Invoke-RestMethod -Uri 'http://127.0.0.1:30001/QueryDB/status' -TimeoutSec 3
    $hookReachable = $true
} catch {
    $hookReachable = $false
}
try {
    $receiverStatus = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/status' -TimeoutSec 3
    $receiverReachable = $true
} catch {
    $receiverReachable = $false
}

$report = [ordered]@{
    weixin_path = $exe
    weixin_version = $version
    version_matches = ($version -eq $expectedVersion)
    signature = $signature
    hook_reachable = $hookReachable
    hook_loopback_only = ($hookListener.Count -gt 0 -and @($hookListener | Where-Object { $_.LocalAddress -notin @('127.0.0.1', '::1') }).Count -eq 0)
    receiver_reachable = $receiverReachable
    receiver_loopback_only = ($receiverListener.Count -gt 0 -and @($receiverListener | Where-Object { $_.LocalAddress -notin @('127.0.0.1', '::1') }).Count -eq 0)
}
if ($hookReachable) {
    $report.is_login = $hookStatus.IsLogin
    $report.sync_batch_calls = $hookStatus.SyncBatchProcessorCalls
    $report.sync_batch_items = $hookStatus.SyncBatchItemCount
    $report.validated_field_reads = $hookStatus.SyncBatchFieldReadCalls
    $report.callback_posts = $hookStatus.MessageCallbackPosts
    $report.last_type = $hookStatus.SyncBatchLastMsgType
    $report.last_from_sha12 = Get-Sha12 ([string]$hookStatus.SyncBatchFromUsername)
    $report.last_from_is_room = ([string]$hookStatus.SyncBatchFromUsername).EndsWith('@chatroom')
}
if ($receiverReachable) {
    $report.receiver_count = $receiverStatus.received
    $report.last_events = $receiverStatus.last_events
}
$report | ConvertTo-Json -Depth 6

if (-not $report.version_matches -or $signature -ne 'Valid') { exit 2 }
if ($RequireHook -and (-not $hookReachable -or -not $report.hook_loopback_only -or
        -not $receiverReachable -or -not $report.receiver_loopback_only)) { exit 3 }
