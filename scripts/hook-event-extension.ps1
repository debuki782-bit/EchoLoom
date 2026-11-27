param(
    [ValidateSet('start','status','stop')][string]$Action = 'status',
    [switch]$ObserveOnly
)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskScript = Join-Path $PSScriptRoot 'run-hook-event-extension.py'
$taskPython = $null
foreach ($candidate in @(
    $env:ECHOLOOM_PYTHON,
    (Join-Path $taskRoot '.venv\Scripts\python.exe')
)) {
    if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) { $taskPython = $candidate; break }
}
if (-not $taskPython) {
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) { $taskPython = $cmd.Source }
}
if (-not $taskPython) { throw '未找到可用 Python 解释器（请创建 .venv 或设置 ECHOLOOM_PYTHON）。' }
$taskOwners = @(Get-NetTCPConnection -LocalPort 30003 -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique)
if ($taskOwners.Count) {
    if ($taskOwners.Count -ne 1) { throw '消息接收扩展端口存在多个进程。' }
    $taskOwner = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $taskOwners[0])
    if (-not $taskOwner.CommandLine -or -not $taskOwner.CommandLine.Contains($taskScript)) {
        throw '端口 30003 的进程不属于本项目的消息接收扩展。'
    }
    if ($Action -eq 'stop') {
        # Let the owning host remove its observers before its process exits.
        Invoke-WebRequest -Method Post 'http://127.0.0.1:30003/stop' `
            -Headers @{'X-Local-Control'='stop'} -TimeoutSec 5 | Out-Null
        for ($taskAttempt = 0; $taskAttempt -lt 20; $taskAttempt++) {
            if (-not (Get-NetTCPConnection -LocalPort 30003 -State Listen -ErrorAction SilentlyContinue)) { break }
            Start-Sleep -Milliseconds 250
        }
        if (Get-NetTCPConnection -LocalPort 30003 -State Listen -ErrorAction SilentlyContinue) {
            throw '消息接收扩展尚未停止完成；没有强制终止微信中的观察器。'
        }
        Write-Output '消息接收扩展已停止。'
        exit
    }
    $taskState = Invoke-RestMethod 'http://127.0.0.1:30003/status' -TimeoutSec 3
    $taskHookOwners = @(Get-NetTCPConnection -LocalPort 30001 -State Listen -ErrorAction Stop |
        Select-Object -ExpandProperty OwningProcess -Unique)
    if ($taskState.service -ne 'wechat-hook-event-extension' -or -not $taskState.ready -or
        $taskHookOwners.Count -ne 1 -or $taskState.pid -ne $taskHookOwners[0]) {
        throw '消息接收扩展连接的微信进程已变化，请先停止该扩展再启动。'
    }
    if ($Action -eq 'start' -and [bool]$taskState.observe_only -ne [bool]$ObserveOnly) {
        throw '消息接收扩展的观察模式不同，请先停止再启动。'
    }
    $taskState | ConvertTo-Json
    exit
}
if ($Action -eq 'stop') { Write-Output '消息接收扩展未运行。'; exit }
if ($Action -eq 'status') { throw '消息接收扩展未运行。' }
$taskAccount = (& $taskPython -c "from pathlib import Path; from echoloom.bootstrap import load_live_route; print(load_live_route(Path(r'$taskRoot')).account)" 2>$null | Select-Object -First 1)
if (-not $taskAccount -and (Test-Path -LiteralPath (Join-Path $taskRoot 'data/live-route.json') -PathType Leaf)) {
    $taskRoute = Get-Content -Raw -LiteralPath (Join-Path $taskRoot 'data/live-route.json') | ConvertFrom-Json
    $taskAccount = [string]$taskRoute.account
}
if (-not $taskAccount) { throw '尚未配置微信账号（请检查 .local/runtime.toml、.local/route.json 或 data/live-route.json）。' }
$taskHookOwners = @(Get-NetTCPConnection -LocalPort 30001 -State Listen -ErrorAction Stop |
    Select-Object -ExpandProperty OwningProcess -Unique)
if ($taskHookOwners.Count -ne 1) { throw '无法唯一确定现有 Hook 进程。' }
$taskWeixin = Get-Process -Id $taskHookOwners[0]
if ($taskWeixin.ProcessName -ne 'Weixin') { throw '现有 Hook 不在微信进程中。' }
$taskModules = @($taskWeixin.Modules | Where-Object { $_.ModuleName -eq 'Weixin.dll' })
if ($taskModules.Count -ne 1) { throw '无法唯一确定微信实际加载的 DLL。' }
$taskDll = $taskModules[0].FileName
if ((Get-FileHash -LiteralPath $taskDll -Algorithm SHA256).Hash -ne
    'F487E4F56930456A25FB0D7B058C77E2DCD364E268D49BDFFF4DC0E3BE26BA36') {
    throw '微信 DLL 与消息接收扩展支持的指纹不匹配。'
}
$taskArguments = @('"' + $taskScript + '"', '--pid', [string]$taskWeixin.Id,
    '--account', [string]$taskAccount, '--wechat-dll', '"' + $taskDll + '"')
if ($ObserveOnly) { $taskArguments += '--observe-only' }
$taskProcess = Start-Process -FilePath $taskPython -ArgumentList $taskArguments `
    -WorkingDirectory $taskRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $taskRoot '.tmp/hook-event-extension.stdout.log') `
    -RedirectStandardError (Join-Path $taskRoot '.tmp/hook-event-extension.stderr.log')
for ($taskAttempt = 0; $taskAttempt -lt 20; $taskAttempt++) {
    Start-Sleep -Milliseconds 500
    try {
        $taskState = Invoke-RestMethod 'http://127.0.0.1:30003/status' -TimeoutSec 1
        if ($taskState.service -eq 'wechat-hook-event-extension' -and $taskState.ready -and $taskState.pid -eq $taskWeixin.Id) {
            $taskState | ConvertTo-Json
            exit
        }
    } catch {}
    if ($taskProcess.HasExited) { break }
}
throw '消息接收扩展启动失败，请查看 .tmp/hook-event-extension.stderr.log。'
