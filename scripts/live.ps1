param([ValidateSet('status','start','stop','pause','resume','restart-worker')][string]$Action='status')
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
$python=$null
foreach($candidate in @($env:ECHOLOOM_PYTHON,(Join-Path $root '.venv-openai\Scripts\python.exe'),(Join-Path $root '.venv\Scripts\python.exe'))){
    if($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)){$python=$candidate;break}
}
if(-not $python){
    $cmd=Get-Command python -ErrorAction SilentlyContinue
    if($cmd){$python=$cmd.Source}
}
if(-not $python){throw 'Python executable not found; create .venv/.venv-openai or set ECHOLOOM_PYTHON'}
$runtimeMetaJson=& $python -m echoloom.local_config runtime-json 2>$null
$runtimeMeta=if($LASTEXITCODE -eq 0 -and $runtimeMetaJson){$runtimeMetaJson | ConvertFrom-Json}else{$null}
$srvCfg=if($runtimeMeta -and $runtimeMeta.server){$runtimeMeta.server}else{[pscustomobject]@{ingress_host='127.0.0.1';ingress_port=8080}}
$ingressHost=if($env:WECHAT_AGENT_HOST){$env:WECHAT_AGENT_HOST}elseif($srvCfg.ingress_host){[string]$srvCfg.ingress_host}else{'127.0.0.1'}
$ingressPort=if($env:WECHAT_AGENT_PORT){[int]$env:WECHAT_AGENT_PORT}elseif($srvCfg.ingress_port){[int]$srvCfg.ingress_port}else{8080}
$agentStatusUrl="http://${ingressHost}:${ingressPort}/status"
$hookCfg=if($runtimeMeta -and $runtimeMeta.wechat_hook){$runtimeMeta.wechat_hook}else{[pscustomobject]@{hook_url='http://127.0.0.1:30001';native_download_url='http://127.0.0.1:30002'}}
$hookUrl=if($hookCfg.hook_url){[string]$hookCfg.hook_url}else{'http://127.0.0.1:30001'}
$hookPort=([Uri]$hookUrl).Port
$nativeDownloadBase=if($hookCfg.native_download_url){[string]$hookCfg.native_download_url}else{'http://127.0.0.1:30002'}
$nativeDownloadStatusUrl=($nativeDownloadBase.TrimEnd('/') + '/status')
$configuredFactory=if($env:ECHOLOOM_GATEWAY_FACTORY){$env:ECHOLOOM_GATEWAY_FACTORY}else{$env:WECHAT_GATEWAY_FACTORY}
$usesWechatSource=if($configuredFactory){
    $configuredFactory -in @('echoloom.adapters.hook:HookGateway','echoloom.connectors.wechat_hook:WeChatHookConnector')
}elseif($runtimeMeta -and $runtimeMeta.sources -and $runtimeMeta.sources.Count -gt 0){
    @($runtimeMeta.sources) -contains 'wechat'
}else{
    $false
}
$ingressScript=Join-Path $root 'scripts\run-ingress.py'
$workerScript=Join-Path $root 'scripts\run-group-pilot.py'
$nativeImageScript=Join-Path $root 'scripts\native-image-download.ps1'
$tmpDir=Join-Path $root '.tmp'
$ingressPidPath=Join-Path $tmpDir 'ingress.pid'
$workerPidPath=Join-Path $tmpDir 'worker.pid'
$legacyPidPath=Join-Path $tmpDir 'pilot.pid'
$ingressStdout=Join-Path $tmpDir 'ingress.stdout.log'
$ingressStderr=Join-Path $tmpDir 'ingress.stderr.log'
$workerStdout=Join-Path $tmpDir 'pilot.stdout.log'
$workerStderr=Join-Path $tmpDir 'pilot.stderr.log'
New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null

function Test-CmdContains([string]$cmd, [string]$needle) {
    if([string]::IsNullOrEmpty($cmd) -or [string]::IsNullOrEmpty($needle)){return $false}
    return $cmd.IndexOf($needle, [System.StringComparison]::OrdinalIgnoreCase) -ge 0
}
function Test-CmdHasRole([string]$cmd, [string]$role) {
    if([string]::IsNullOrEmpty($cmd)){return $false}
    return [bool]($cmd -match ('(^|\s)--role(\s+|=)' + [regex]::Escape($role) + '(\s|$)'))
}
function Get-HookListenerPid {
    if(-not $usesWechatSource){return $null}
    $ids=@(Get-NetTCPConnection -LocalPort $hookPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique)
    if($ids.Count -eq 1){return [int]$ids[0]}
    return $null
}
function Get-NativeImageDownloadReady {
    if(-not $usesWechatSource){return $false}
    try {
        $natState=Invoke-RestMethod $nativeDownloadStatusUrl -TimeoutSec 1
        if($natState.service -ne 'wechat-native-image-download' -or [int]$natState.pid -le 0){return $false}
        $hookPid=Get-HookListenerPid
        return [bool]($null -eq $hookPid -or [int]$natState.pid -eq $hookPid)
    } catch {
        return $false
    }
}
function Ensure-NativeImageDownload {
    if(-not $usesWechatSource){return $false}
    if(Get-NativeImageDownloadReady){return $true}
    if(Test-Path -LiteralPath $nativeImageScript){
        try {
            & $nativeImageScript -Action start | Out-Null
        } catch {}
    }
    return (Get-NativeImageDownloadReady)
}
function Get-PythonProcesses {
    @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -in @('python.exe','pythonw.exe') -and $_.CommandLine
    })
}
function Get-IngressProcesses {
    @(Get-PythonProcesses | Where-Object {
        (Test-CmdContains $_.CommandLine '--run') -and (
            (Test-CmdContains $_.CommandLine $ingressScript) -or
            ((Test-CmdContains $_.CommandLine $workerScript) -and (Test-CmdHasRole $_.CommandLine 'ingress'))
        )
    })
}
function Get-WorkerProcesses {
    @(Get-PythonProcesses | Where-Object {
        (Test-CmdContains $_.CommandLine $workerScript) -and
        (Test-CmdContains $_.CommandLine '--run') -and
        (Test-CmdHasRole $_.CommandLine 'worker')
    })
}
function Get-LegacyPilotProcesses {
    @(Get-PythonProcesses | Where-Object {
        (Test-CmdContains $_.CommandLine $workerScript) -and
        (Test-CmdContains $_.CommandLine '--run') -and
        (-not (Test-CmdHasRole $_.CommandLine 'worker')) -and
        (-not (Test-CmdHasRole $_.CommandLine 'ingress'))
    })
}
function Get-AllAgentProcesses {
    $seen=@{}
    $list=@()
    foreach($proc in (@(Get-IngressProcesses) + @(Get-WorkerProcesses) + @(Get-LegacyPilotProcesses))){
        if(-not $seen.ContainsKey($proc.ProcessId)){
            $seen[$proc.ProcessId]=$true
            $list+=$proc
        }
    }
    return $list
}
function Get-PilotListenerIds {
    @(Get-NetTCPConnection -LocalPort $ingressPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique)
}
function Stop-ProcessList($processes) {
    foreach($process in ($processes | Sort-Object CreationDate -Descending)){
        $stillRunning=Get-CimInstance Win32_Process -Filter ('ProcessId='+$process.ProcessId)
        if($stillRunning){Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue}
    }
}
function Sync-WorkerPidFile {
    $workers=Get-WorkerProcesses
    if($workers.Count -gt 0){
        $latest=($workers | Sort-Object CreationDate -Descending | Select-Object -First 1)
        $latest.ProcessId | Set-Content -LiteralPath $workerPidPath
        return [int]$latest.ProcessId
    }
    if(Test-Path -LiteralPath $workerPidPath){Remove-Item -LiteralPath $workerPidPath -Force}
    return $null
}
function Get-AgentStatusOrNull([int]$timeoutSec=2) {
    try {
        $state=Invoke-RestMethod $agentStatusUrl -TimeoutSec $timeoutSec
        if($state.mode -eq 'selfhosted_group'){return $state}
    } catch {}
    return $null
}
function Format-StatusOutput($state, [int]$ingressPid=0, $workerPid=$null, [string]$architecture='scheme_a') {
    $state | Add-Member -NotePropertyName 'native_image_download_ready' -NotePropertyValue (Get-NativeImageDownloadReady) -Force
    $state | Add-Member -NotePropertyName 'architecture' -NotePropertyValue $architecture -Force
    if($ingressPid -gt 0){
        $state | Add-Member -NotePropertyName 'ingress_pid' -NotePropertyValue $ingressPid -Force
    }
    if($null -ne $workerPid){
        $state | Add-Member -NotePropertyName 'worker_pid' -NotePropertyValue ([int]$workerPid) -Force
    }
    $state | ConvertTo-Json -Depth 6
}
function Start-IngressProcess {
    if(Get-PilotListenerIds){
        throw 'Port 8080 belongs to another service; inspect before starting'
    }
    $proc=Start-Process -FilePath $python -ArgumentList @($ingressScript,'--run') -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput $ingressStdout -RedirectStandardError $ingressStderr -PassThru
    $proc.Id | Set-Content -LiteralPath $ingressPidPath
    for($i=0;$i -lt 20;$i++){
        Start-Sleep -Milliseconds 500
        $state=Get-AgentStatusOrNull 1
        if($null -ne $state){
            $listeners=Get-PilotListenerIds
            $ingressProcs=Get-IngressProcesses
            if($listeners.Count -ne 1 -or $listeners[0] -notin @($ingressProcs | Select-Object -ExpandProperty ProcessId)){
                throw 'Started Ingress listener PID could not be verified'
            }
            $listeners[0] | Set-Content -LiteralPath $ingressPidPath
            return [int]$listeners[0]
        }
        if($proc.HasExited -and (Get-IngressProcesses).Count -eq 0){break}
    }
    throw 'Ingress startup failed; inspect .tmp/ingress.stderr.log'
}
function Start-WorkerProcess {
    $proc=Start-Process -FilePath $python -ArgumentList @($workerScript,'--run','--role','worker') -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput $workerStdout -RedirectStandardError $workerStderr -PassThru
    $proc.Id | Set-Content -LiteralPath $workerPidPath
    for($i=0;$i -lt 30;$i++){
        Start-Sleep -Seconds 1
        $state=Get-AgentStatusOrNull 2
        if($null -ne $state -and [bool]$state.worker_alive){
            $verifiedWorkerPid=Sync-WorkerPidFile
            if($null -eq $verifiedWorkerPid){$verifiedWorkerPid=$proc.Id}
            return @{ State=$state; WorkerPid=$verifiedWorkerPid }
        }
        if($proc.HasExited -and (Get-WorkerProcesses).Count -eq 0){break}
    }
    throw 'Worker startup failed; inspect .tmp/pilot.stderr.log'
}
function Stop-OnlyWorkerProcesses {
    $workers=Get-WorkerProcesses
    if($workers.Count -gt 0){
        Stop-ProcessList $workers
        for($i=0;$i -lt 20;$i++){
            if(-not (Get-WorkerProcesses).Count){break}
            Start-Sleep -Milliseconds 250
        }
        if((Get-WorkerProcesses).Count){
            throw 'Worker process tree did not stop cleanly'
        }
    }
    if(Test-Path -LiteralPath $workerPidPath){Remove-Item -LiteralPath $workerPidPath -Force}
}
function Stop-AllAgentProcesses {
    $processes=Get-AllAgentProcesses
    if(-not $processes.Count){
        if(Get-PilotListenerIds){throw 'Port 8080 is occupied by a process not matching this Agent; refusing stop'}
        foreach($path in @($ingressPidPath,$workerPidPath,$legacyPidPath)){
            if(Test-Path -LiteralPath $path){Remove-Item -LiteralPath $path -Force}
        }
        return 0
    }
    $agentIds=@($processes | Select-Object -ExpandProperty ProcessId)
    foreach($listenerId in (Get-PilotListenerIds)){
        if($listenerId -notin $agentIds){throw 'Port 8080 owner does not match this Agent; refusing stop'}
    }
    Stop-ProcessList $processes
    for($i=0;$i -lt 20;$i++){
        if(-not (Get-AllAgentProcesses).Count -and -not (Get-PilotListenerIds)){break}
        Start-Sleep -Milliseconds 250
    }
    if((Get-AllAgentProcesses).Count -or (Get-PilotListenerIds)){
        throw 'Agent process tree did not stop cleanly'
    }
    foreach($path in @($ingressPidPath,$workerPidPath,$legacyPidPath)){
        if(Test-Path -LiteralPath $path){Remove-Item -LiteralPath $path -Force}
    }
    return $agentIds.Count
}

if($Action -in @('pause','resume')) {
    $paused=if($Action -eq 'pause'){1}else{0}
    $database=Join-Path $root 'data\pilot-receipts.sqlite3'
    & $python -c 'import sqlite3,sys; db=sqlite3.connect(sys.argv[1], timeout=5.0); db.execute("UPDATE control SET paused=? WHERE id=1",(int(sys.argv[2]),)); db.commit(); print("paused="+sys.argv[2])' $database $paused
    exit $LASTEXITCODE
}

if($Action -eq 'stop') {
    $stoppedCount=Stop-AllAgentProcesses
    if($stoppedCount -eq 0){
        Write-Output 'Agent already stopped'
    } else {
        Write-Output ('Stopped verified Agent process tree ('+$stoppedCount+' processes); WeChat was not restarted')
    }
    exit
}

if($Action -eq 'restart-worker') {
    $listeners=Get-PilotListenerIds
    $ingressProcs=Get-IngressProcesses
    $legacyProcs=Get-LegacyPilotProcesses
    $ingressIds=@($ingressProcs | Select-Object -ExpandProperty ProcessId)
    $legacyIds=@($legacyProcs | Select-Object -ExpandProperty ProcessId)

    if($listeners.Count -eq 1 -and $listeners[0] -in $ingressIds){
        # Scheme A fast path: keep :8080 Ingress alive and uninterrupted; restart ONLY Worker.
        $ingressPid=[int]$listeners[0]
        $ingressPid | Set-Content -LiteralPath $ingressPidPath
        Stop-OnlyWorkerProcesses
        $started=Start-WorkerProcess
        [void](Ensure-NativeImageDownload)
        Format-StatusOutput $started.State $ingressPid $started.WorkerPid 'scheme_a'
        exit
    }

    if($legacyProcs.Count -gt 0 -and ($listeners.Count -eq 0 -or ($listeners.Count -eq 1 -and $listeners[0] -in $legacyIds))){
        # Upgrade legacy single-process instance cleanly into Scheme A dual-process.
        [void](Stop-AllAgentProcesses)
    } elseif($listeners.Count -gt 0){
        throw 'Port 8080 owner does not match this Agent; refusing restart-worker'
    } else {
        Stop-OnlyWorkerProcesses
    }

    $ingressPid=Start-IngressProcess
    $started=Start-WorkerProcess
    [void](Ensure-NativeImageDownload)
    Format-StatusOutput $started.State $ingressPid $started.WorkerPid 'scheme_a'
    exit
}

$liveState=Get-AgentStatusOrNull 2
if($null -ne $liveState){
    $listeners=Get-PilotListenerIds
    $ingressProcs=Get-IngressProcesses
    $legacyProcs=Get-LegacyPilotProcesses
    $ingressIds=@($ingressProcs | Select-Object -ExpandProperty ProcessId)
    $legacyIds=@($legacyProcs | Select-Object -ExpandProperty ProcessId)
    $isIngress=($listeners.Count -eq 1 -and $listeners[0] -in $ingressIds)
    $isLegacy=($listeners.Count -eq 1 -and $listeners[0] -in $legacyIds)
    if(-not $isIngress -and -not $isLegacy){
        throw 'Agent status is live but the listener PID does not match this project'
    }
    if($isLegacy){
        $listeners[0] | Set-Content -LiteralPath $legacyPidPath
        if($Action -eq 'start'){[void](Ensure-NativeImageDownload)}
        Format-StatusOutput $liveState ([int]$listeners[0]) ([int]$listeners[0]) 'single_process'
        exit
    }
    $ingressPid=[int]$listeners[0]
    $ingressPid | Set-Content -LiteralPath $ingressPidPath
    $workerPid=Sync-WorkerPidFile
    if($Action -eq 'status'){
        Format-StatusOutput $liveState $ingressPid $workerPid 'scheme_a'
        exit
    }
    # Action -eq 'start' with live Ingress: ensure Worker is also up without disturbing Ingress.
    if($null -ne $workerPid -and [bool]$liveState.worker_alive){
        [void](Ensure-NativeImageDownload)
        Format-StatusOutput $liveState $ingressPid $workerPid 'scheme_a'
        exit
    }
    Stop-OnlyWorkerProcesses
    $started=Start-WorkerProcess
    [void](Ensure-NativeImageDownload)
    Format-StatusOutput $started.State $ingressPid $started.WorkerPid 'scheme_a'
    exit
}

if($Action -eq 'status'){throw 'Selfhosted Agent is not available'}

# Action -eq 'start' when :8080 is not up: clean up any stray worker/legacy processes safely, then start Scheme A pair.
if(Get-PilotListenerIds){
    throw 'Port 8080 belongs to another service; inspect before starting'
}
if((Get-LegacyPilotProcesses).Count -gt 0 -or (Get-IngressProcesses).Count -gt 0){
    [void](Stop-AllAgentProcesses)
}
$ingressPid=Start-IngressProcess
$workerPid=Sync-WorkerPidFile
$postIngressState=Get-AgentStatusOrNull 2
if($null -ne $workerPid -and $null -ne $postIngressState -and [bool]$postIngressState.worker_alive){
    [void](Ensure-NativeImageDownload)
    Format-StatusOutput $postIngressState $ingressPid $workerPid 'scheme_a'
    exit
}
Stop-OnlyWorkerProcesses
$started=Start-WorkerProcess
[void](Ensure-NativeImageDownload)
Format-StatusOutput $started.State $ingressPid $started.WorkerPid 'scheme_a'
