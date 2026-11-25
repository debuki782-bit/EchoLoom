param(
    [switch]$Send,
    [string]$ExpectedRoomSha = $env:ECHOLOOM_PROBE_ROOM_SHA
)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$receiptPath = Join-Path $root '.tmp\image-probe-receipt-20261029.json'
if (Test-Path -LiteralPath $receiptPath) {
    Get-Content -LiteralPath $receiptPath
    if ($Send) { throw 'Attempt already recorded; do not retry an uncertain send.' }
    exit
}
if (-not $Send) { Write-Output 'No recorded image attempt.'; exit }
if (-not $ExpectedRoomSha) { throw 'Pass -ExpectedRoomSha or set ECHOLOOM_PROBE_ROOM_SHA.' }
$status = Invoke-RestMethod 'http://127.0.0.1:30001/QueryDB/status' -TimeoutSec 5
$room = [string]$status.SyncBatchFromUsername
$sha = [Security.Cryptography.SHA256]::Create()
try {
    $targetSha = [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($room))).Replace('-', '').Substring(0, 12).ToLowerInvariant()
} finally { $sha.Dispose() }
if ($status.IsLogin -ne 1 -or $targetSha -ne $ExpectedRoomSha -or -not $room.EndsWith('@chatroom')) {
    throw 'Logged-in test group could not be confirmed.'
}
$png = Join-Path $root '.tmp\codex-image-probe-20261029.png'
$hash = (Get-FileHash -LiteralPath $png -Algorithm SHA256).Hash.ToLowerInvariant()
if ($hash -ne '36bead16c4fe06fb7d2d085bcffd5e86368a2e088fe41be5f6cafc539d601ab3') {
    throw 'Probe PNG hash mismatch.'
}
$receipt = [ordered]@{
    attempted_at = [DateTimeOffset]::Now.ToString('o')
    target_sha = $targetSha
    png_sha256 = $hash
    state = 'attempt_started_delivery_unknown'
    ret = $null
    queued = $null
    delivered = $false
}
# CreateNew persists intent before the side effect, including across interruptions.
$stream = [IO.File]::Open($receiptPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
try {
    $bytes = [Text.Encoding]::UTF8.GetBytes(($receipt | ConvertTo-Json))
    $stream.Write($bytes, 0, $bytes.Length)
    $stream.Flush($true)
} finally { $stream.Dispose() }
$body = @{ wxidorgid = $room; path = $png } | ConvertTo-Json -Compress
try {
    $response = Invoke-RestMethod 'http://127.0.0.1:30001/SendImgMsg' -Method Post -ContentType 'application/json' -Body ([Text.Encoding]::UTF8.GetBytes($body)) -TimeoutSec 10
    $receipt.ret = $response.ret
    $receipt.queued = $response.queued
    $receipt.state = 'api_returned_delivery_unconfirmed'
} catch {
    $receipt.state = 'transport_error_delivery_unknown'
} finally {
    $receipt | ConvertTo-Json | Set-Content -LiteralPath $receiptPath -Encoding utf8
}
$receipt | ConvertTo-Json -Compress
