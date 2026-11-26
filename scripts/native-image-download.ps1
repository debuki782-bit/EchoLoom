param(
    [ValidateSet('start','status','stop')][string]$Action = 'status',
    [string]$AdapterDll = ''
)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskPython = $null
foreach ($candidate in @(
    $env:ECHOLOOM_PYTHON,
    (Join-Path $taskRoot '.venv-openai\Scripts\python.exe'),
    (Join-Path $taskRoot '.venv\Scripts\python.exe')
)) {
    if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) { $taskPython = $candidate; break }
}
if (-not $taskPython) {
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) { $taskPython = $cmd.Source }
}
if (-not $taskPython) { throw 'Python executable not found; create .venv/.venv-openai or set ECHOLOOM_PYTHON.' }
$taskScript = Join-Path $taskRoot 'scripts/run-native-image-download.py'
$taskTemp = Join-Path $taskRoot '.tmp'
New-Item -ItemType Directory -Force -Path $taskTemp | Out-Null
$taskListeners = @(Get-NetTCPConnection -LocalPort 30002 -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique)
if ($taskListeners.Count) {
    if ($taskListeners.Count -ne 1) { throw 'Multiple processes are listening on port 30002.' }
    $taskOwner = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $taskListeners[0])
    if (-not $taskOwner.CommandLine -or -not $taskOwner.CommandLine.Contains($taskScript)) {
        throw 'Port 30002 owner does not match run-native-image-download.py.'
    }
    if ($Action -eq 'stop') {
        Stop-Process -Id $taskOwner.ProcessId -ErrorAction Stop
        Write-Output 'Stopped native image download service.'
        exit
    }
    $taskState = Invoke-RestMethod 'http://127.0.0.1:30002/status' -TimeoutSec 3
    $taskHookOwner = @(Get-NetTCPConnection -LocalPort 30001 -State Listen -ErrorAction Stop |
        Select-Object -ExpandProperty OwningProcess -Unique)
    if ($taskState.service -ne 'wechat-native-image-download' -or
        $taskHookOwner.Count -ne 1 -or $taskState.pid -ne $taskHookOwner[0]) {
        throw 'Native image download service is attached to a stale WeChat PID; stop it before restarting.'
    }
    $taskState | ConvertTo-Json
    exit
}
if ($Action -eq 'stop') { Write-Output 'Native image download service is already stopped.'; exit }
if ($Action -eq 'status') { throw 'Native image download service is not running.' }
if (-not $AdapterDll) {
    $AdapterDll = Join-Path $taskRoot '.runtime/native-image-download/wx_image_download_411027.dll'
}
$AdapterDll = (Resolve-Path -LiteralPath $AdapterDll).Path
$taskAccount = (& $taskPython -c "from pathlib import Path; from echoloom.bootstrap import load_live_route; print(load_live_route(Path(r'$taskRoot')).account)" 2>$null | Select-Object -First 1)
if (-not $taskAccount -and (Test-Path -LiteralPath (Join-Path $taskRoot 'data/live-route.json') -PathType Leaf)) {
    $taskRoute = Get-Content -Raw -LiteralPath (Join-Path $taskRoot 'data/live-route.json') | ConvertFrom-Json
    $taskAccount = [string]$taskRoute.account
}
if (-not $taskAccount) { throw 'WeChat account is not configured in .local/runtime.toml, .local/route.json, or data/live-route.json.' }
$taskHookOwners = @(Get-NetTCPConnection -LocalPort 30001 -State Listen -ErrorAction Stop |
    Select-Object -ExpandProperty OwningProcess -Unique)
if ($taskHookOwners.Count -ne 1) { throw 'Could not uniquely identify Hook listener on port 30001.' }
$taskWeixin = Get-Process -Id $taskHookOwners[0]
if ($taskWeixin.ProcessName -ne 'Weixin') { throw 'Port 30001 owner is not Weixin.exe.' }
$taskModules = @($taskWeixin.Modules | Where-Object { $_.ModuleName -eq 'Weixin.dll' })
if ($taskModules.Count -ne 1) { throw 'Could not uniquely resolve loaded Weixin.dll module.' }
$taskDllPath = $taskModules[0].FileName
if ((Get-FileHash -LiteralPath $taskDllPath -Algorithm SHA256).Hash -ne
    'F487E4F56930456A25FB0D7B058C77E2DCD364E268D49BDFFF4DC0E3BE26BA36') {
    throw 'Loaded Weixin.dll SHA256 does not match supported fingerprint.'
}
$taskArguments = @('"' + $taskScript + '"', '--pid', [string]$taskWeixin.Id,
    '--account', [string]$taskAccount, '--wechat-dll', '"' + $taskDllPath + '"',
    '--adapter-dll', '"' + $AdapterDll + '"')
$taskProcess = Start-Process -FilePath $taskPython -ArgumentList $taskArguments `
    -WorkingDirectory $taskRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $taskTemp 'native-image-download.stdout.log') `
    -RedirectStandardError (Join-Path $taskTemp 'native-image-download.stderr.log')
for ($taskAttempt = 0; $taskAttempt -lt 20; $taskAttempt++) {
    Start-Sleep -Milliseconds 500
    try {
        $taskState = Invoke-RestMethod 'http://127.0.0.1:30002/status' -TimeoutSec 1
        if ($taskState.service -eq 'wechat-native-image-download' -and $taskState.pid -eq $taskWeixin.Id) {
            $taskState | ConvertTo-Json
            exit
        }
    } catch {}
    if ($taskProcess.HasExited) { break }
}
throw 'Native image download service failed to start; inspect .tmp/native-image-download.stderr.log.'
