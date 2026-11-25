param(
    [Parameter(Mandatory=$true)][ValidateRange(0, 2147483647)][int]$BaselineReceiverCount,
    [Parameter(Mandatory=$true)][ValidateRange(0, 2147483647)][int]$BaselineCallbackPosts
)

$ErrorActionPreference = 'Stop'
$receiver = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/status' -TimeoutSec 3
$hook = Invoke-RestMethod -Uri 'http://127.0.0.1:30001/QueryDB/status' -TimeoutSec 3

$receivedDelta = [int]$receiver.received - $BaselineReceiverCount
$callbackDelta = [int]$hook.MessageCallbackPosts - $BaselineCallbackPosts
$recent = @($receiver.last_events)
$windowComplete = ($receivedDelta -ge 0 -and $receivedDelta -le $recent.Count)
$newEvents = @()
if ($windowComplete -and $receivedDelta -gt 0) {
    $newEvents = @($recent | Select-Object -Last $receivedDelta)
}
$groups = @($newEvents | Where-Object { $_.is_group -eq $true })
$groupTexts = @($groups | Where-Object { $_.msgtype -eq 1 -and $_.sender_is_room -eq $false })
$rooms = @($groupTexts | ForEach-Object { $_.room_sha } | Where-Object { $_ -and $_ -ne 'missing' } | Sort-Object -Unique)
$senders = @($groupTexts | ForEach-Object { $_.sender_sha } | Where-Object { $_ -and $_ -ne 'missing' } | Sort-Object -Unique)

$report = [ordered]@{
    receiver_count = [int]$receiver.received
    receiver_delta = $receivedDelta
    hook_callback_posts = [int]$hook.MessageCallbackPosts
    hook_callback_delta = $callbackDelta
    window_complete = $windowComplete
    group_events_in_window = $groups.Count
    group_text_events_in_window = $groupTexts.Count
    distinct_room_count = $rooms.Count
    distinct_sender_candidate_count = $senders.Count
    same_room_for_two_senders = ($groupTexts.Count -ge 2 -and $rooms.Count -eq 1 -and $senders.Count -ge 2)
    group_events = @($groups | Select-Object received_at,room_sha,sender_sha,sender_origin,from_is_room,sender_is_room,msgtype,msgsvrid,source_id_nonzero,native_mention_ids_present,payload_text_length)
    sender_identity_authenticated = $false
    source_event_id_stable = $false
    group_commands_ready = $false
}
$report | ConvertTo-Json -Depth 5

if (-not $windowComplete) { exit 2 }
