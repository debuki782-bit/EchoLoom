param(
    [ValidateRange(30, 900)]
    [int]$LoginWaitSeconds = 300,
    [switch]$CheckOnly
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$python = $null
foreach ($candidate in @(
    $env:ECHOLOOM_PYTHON,
    (Join-Path $root '.venv-openai\Scripts\python.exe'),
    (Join-Path $root '.venv\Scripts\python.exe')
)) {
    if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
        $python = $candidate
        break
    }
}
if (-not $python) {
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) { $python = $cmd.Source }
}
$dashboardScript = Join-Path $root 'scripts\trace-dashboard.py'
$liveScript = Join-Path $PSScriptRoot 'live.ps1'
$tempRoot = Join-Path $root '.tmp'
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
$logPath = Join-Path $tempRoot 'start-services.log'
$transcriptStarted = $false
try {
    Start-Transcript -Path $logPath -Append -ErrorAction Stop | Out-Null
    $transcriptStarted = $true
} catch {}
Write-Host ("[{0}] 开始执行服务启动与健康检查（日志：{1}）" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $logPath)
$hookPluginScript = Join-Path $PSScriptRoot 'hooks\wechat-4.1.10.27.ps1'

if (-not $python -or -not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "未找到可用的 Python 解释器（请创建 .venv/.venv-openai 或设置 ECHOLOOM_PYTHON）。"
}
if (-not (Test-Path -LiteralPath $dashboardScript -PathType Leaf)) {
    throw "管理页启动脚本不存在：$dashboardScript"
}

$runtimeMetaJson = & $python -m echoloom.local_config runtime-json 2>$null
$runtimeMeta = if ($LASTEXITCODE -eq 0 -and $runtimeMetaJson) { $runtimeMetaJson | ConvertFrom-Json } else { $null }
$srvCfg = if ($runtimeMeta -and $runtimeMeta.server) { $runtimeMeta.server } else { [pscustomobject]@{
    ingress_host = '127.0.0.1'
    ingress_port = 8080
    dashboard_host = '127.0.0.1'
    dashboard_port = 8767
} }
$ingressHost = if ($env:WECHAT_AGENT_HOST) { $env:WECHAT_AGENT_HOST } elseif ($srvCfg.ingress_host) { [string]$srvCfg.ingress_host } else { '127.0.0.1' }
$ingressPort = if ($env:WECHAT_AGENT_PORT) { [int]$env:WECHAT_AGENT_PORT } elseif ($srvCfg.ingress_port) { [int]$srvCfg.ingress_port } else { 8080 }
$dashboardHost = if ($srvCfg.dashboard_host) { [string]$srvCfg.dashboard_host } else { '127.0.0.1' }
$dashboardPort = if ($srvCfg.dashboard_port) { [int]$srvCfg.dashboard_port } else { 8767 }
$dashboardUrl = "http://${dashboardHost}:${dashboardPort}"
$agentStatusUrl = "http://${ingressHost}:${ingressPort}/status"
$hookCfg = if ($runtimeMeta -and $runtimeMeta.wechat_hook) { $runtimeMeta.wechat_hook } else { [pscustomobject]@{
    hook_url = 'http://127.0.0.1:30001'
    native_download_url = 'http://127.0.0.1:30002'
    event_extension_url = 'http://127.0.0.1:30003'
    native_download_dll = '.runtime/native-image-download/wx_image_download_411027.dll'
} }
$hookBaseUrl = if ($env:WECHAT_HOOK_URL) { $env:WECHAT_HOOK_URL } elseif ($hookCfg.hook_url) { [string]$hookCfg.hook_url } else { 'http://127.0.0.1:30001' }
$hookStatusUrl = ($hookBaseUrl.TrimEnd('/') + '/QueryDB/status')
$hookPort = ([Uri]$hookBaseUrl).Port
$nativeDownloadUrl = if ($hookCfg.native_download_url) { [string]$hookCfg.native_download_url } else { 'http://127.0.0.1:30002' }
$eventExtensionUrl = if ($hookCfg.event_extension_url) { [string]$hookCfg.event_extension_url } else { 'http://127.0.0.1:30003' }
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null

function Test-TraceDashboard {
    try {
        $overview = Invoke-RestMethod "$dashboardUrl/api/overview" -TimeoutSec 3
        return ($null -ne $overview -and
            $overview.PSObject.Properties.Name -contains 'trace_records' -and
            $overview.PSObject.Properties.Name -contains 'conversations')
    } catch {
        return $false
    }
}

function Get-HookStatus {
    try {
        return Invoke-RestMethod $hookStatusUrl -TimeoutSec 3
    } catch {
        return $null
    }
}

function Test-IsLoggedIn($status) {
    return ($null -ne $status -and
        $status.PSObject.Properties.Name -contains 'IsLogin' -and
        [string]$status.IsLogin -eq '1')
}

function Ensure-HookExtension($scriptPath, $label, [bool]$required = $false) {
    if (-not (Test-Path -LiteralPath $scriptPath -PathType Leaf)) { return }
    try {
        & $scriptPath -Action start | Out-Null
    } catch {
        $firstError = $_.Exception.Message
        try {
            & $scriptPath -Action stop | Out-Null
            & $scriptPath -Action start | Out-Null
            Write-Host "$label 已自动清理旧会话并重新启动。"
        } catch {
            if ($required) {
                throw "$label 启动失败：$($_.Exception.Message)（初次错误：$firstError）"
            }
            Write-Warning "$label 未就绪：$($_.Exception.Message)"
        }
    }
}

function Invoke-HealthCheck([bool]$customGateway) {
    $results = @()
    $allHealthy = $true

    # 1. Trace Dashboard
    try {
        $overview = Invoke-RestMethod "$dashboardUrl/api/overview" -TimeoutSec 8
        $ok = ($null -ne $overview -and $overview.PSObject.Properties.Name -contains 'trace_records')
        $detail = if ($ok) { "traces=$($overview.trace_records)" } else { '响应格式异常' }
        if (-not $ok) { $allHealthy = $false }
        $results += [pscustomobject]@{ Name="Trace Dashboard ($dashboardPort)"; Ok=$ok; Detail=$detail; Url="$dashboardUrl/" }
    } catch {
        $allHealthy = $false
        $results += [pscustomobject]@{ Name="Trace Dashboard ($dashboardPort)"; Ok=$false; Detail=$_.Exception.Message; Url="$dashboardUrl/" }
    }

    $hookPid = $null
    if (-not $customGateway) {
        # 2. WeChat Hook (optional desktop source plugin)
        try {
            $hookListeners = @(Get-NetTCPConnection -LocalPort $hookPort -State Listen -ErrorAction SilentlyContinue |
                Select-Object -ExpandProperty OwningProcess -Unique)
            $hookState = Invoke-RestMethod $hookStatusUrl -TimeoutSec 3
            $loggedIn = Test-IsLoggedIn $hookState
            if ($hookListeners.Count -eq 1) { $hookPid = [int]$hookListeners[0] }
            $ok = ($loggedIn -and $null -ne $hookPid)
            $detail = "IsLogin=$($hookState.IsLogin), pid=$hookPid, callbacks=$($hookState.MessageCallbackPosts)"
            if (-not $ok) { $allHealthy = $false }
            $results += [pscustomobject]@{ Name="WeChat Hook ($hookPort)"; Ok=$ok; Detail=$detail; Url=$hookStatusUrl }
        } catch {
            $allHealthy = $false
            $results += [pscustomobject]@{ Name="WeChat Hook ($hookPort)"; Ok=$false; Detail=$_.Exception.Message; Url=$hookStatusUrl }
        }

        # 3. Native Image Download
        $nativeDllRel = if ($hookCfg.native_download_dll) { [string]$hookCfg.native_download_dll } else { '.runtime/native-image-download/wx_image_download_411027.dll' }
        $nativeDll = if ([IO.Path]::IsPathRooted($nativeDllRel)) { $nativeDllRel } else { Join-Path $root $nativeDllRel }
        if (Test-Path -LiteralPath $nativeDll -PathType Leaf) {
            try {
                $natState = Invoke-RestMethod ($nativeDownloadUrl.TrimEnd('/') + '/status') -TimeoutSec 2
                $pidMatch = ($null -eq $hookPid -or [int]$natState.pid -eq $hookPid)
                $ok = ($natState.service -eq 'wechat-native-image-download' -and [int]$natState.pid -gt 0 -and $pidMatch)
                $detail = "pid=$($natState.pid), callback_ready=$($natState.callback_ready)"
                if (-not $ok) { $allHealthy = $false }
                $results += [pscustomobject]@{ Name='原图下载扩展'; Ok=$ok; Detail=$detail; Url=($nativeDownloadUrl.TrimEnd('/') + '/status') }
            } catch {
                $allHealthy = $false
                $results += [pscustomobject]@{ Name='原图下载扩展'; Ok=$false; Detail=$_.Exception.Message; Url=($nativeDownloadUrl.TrimEnd('/') + '/status') }
            }
        }

        # 4. Hook Event Extension (Bilibili long card & pat)
        if ($hookCfg.weixin_dll_sha256) {
            try {
                $extState = Invoke-RestMethod ($eventExtensionUrl.TrimEnd('/') + '/status') -TimeoutSec 2
                $pidMatch = ($null -eq $hookPid -or [int]$extState.pid -eq $hookPid)
                $ok = ($extState.service -eq 'wechat-hook-event-extension' -and [bool]$extState.ready -and $pidMatch)
                $detail = "pid=$($extState.pid), ready=$($extState.ready), rx=$($extState.received), tx=$($extState.posted), err=$($extState.read_errors)"
                if (-not $ok) { $allHealthy = $false }
                $results += [pscustomobject]@{ Name='长卡片/拍一拍扩展'; Ok=$ok; Detail=$detail; Url=($eventExtensionUrl.TrimEnd('/') + '/status') }
            } catch {
                $allHealthy = $false
                $results += [pscustomobject]@{ Name='长卡片/拍一拍扩展'; Ok=$false; Detail=$_.Exception.Message; Url=($eventExtensionUrl.TrimEnd('/') + '/status') }
            }
        }
    }

    # 5. Agent Ingress + Worker
    try {
        $agent = Invoke-RestMethod $agentStatusUrl -TimeoutSec 3
        $workerAlive = ($agent.PSObject.Properties.Name -notcontains 'worker_alive' -or
            $null -eq $agent.worker_alive -or [bool]$agent.worker_alive)
        $loginWatchAlive = ($agent.PSObject.Properties.Name -notcontains 'login_watch_alive' -or
            $null -eq $agent.login_watch_alive -or [bool]$agent.login_watch_alive)
        $trustedOk = ($customGateway -or [bool]$agent.trusted_gateway)
        $ok = ($agent.mode -eq 'selfhosted_group' -and $workerAlive -and $loginWatchAlive -and
            [bool]$agent.provider_ready -and -not [bool]$agent.paused -and $trustedOk)
        $detail = ("mode={0}, worker={1}, trusted={2}, provider={3}, paused={4}, queue={5}, rx={6}" -f
            $agent.mode, $workerAlive, $agent.trusted_gateway, $agent.provider_ready,
            $agent.paused, $agent.queue, $agent.received)
        if (-not $ok) { $allHealthy = $false }
        $results += [pscustomobject]@{ Name="Agent Ingress+Worker ($ingressPort)"; Ok=$ok; Detail=$detail; Url=$agentStatusUrl }
    } catch {
        $allHealthy = $false
        $results += [pscustomobject]@{ Name="Agent Ingress+Worker ($ingressPort)"; Ok=$false; Detail=$_.Exception.Message; Url=$agentStatusUrl }
    }

    Write-Host ''
    Write-Host '================ 服务健康检查汇总 ================'
    foreach ($item in $results) {
        $tag = if ($item.Ok) { '[ OK ]' } else { '[FAIL]' }
        $color = if ($item.Ok) { 'Green' } else { 'Red' }
        Write-Host ("  {0} {1,-26} | {2}" -f $tag, $item.Name, $item.Detail) -ForegroundColor $color
    }
    Write-Host '=================================================='
    return $allHealthy
}

$configuredFactory = if ($env:ECHOLOOM_GATEWAY_FACTORY) { $env:ECHOLOOM_GATEWAY_FACTORY } else { $env:WECHAT_GATEWAY_FACTORY }
$usesWechatSource = if ($configuredFactory) {
    $configuredFactory -in @('echoloom.adapters.hook:HookGateway', 'echoloom.connectors.wechat_hook:WeChatHookConnector')
} elseif ($runtimeMeta -and $runtimeMeta.sources -and $runtimeMeta.sources.Count -gt 0) {
    @($runtimeMeta.sources) -contains 'wechat'
} else {
    [bool]($hookCfg.launcher_exe)
}
$customGateway = (-not $usesWechatSource) -or `
    ($env:WECHAT_HOOK_URL -and $env:WECHAT_HOOK_URL.TrimEnd('/') -ne 'http://127.0.0.1:30001')

$exitCode = 0
try {
    if (-not $CheckOnly) {
        # Start the local trace dashboard once, and never take over an unrelated port owner.
        $dashboardListeners = @(
            Get-NetTCPConnection -LocalPort $dashboardPort -State Listen -ErrorAction SilentlyContinue |
                Select-Object -ExpandProperty OwningProcess -Unique
        )
        if ($dashboardListeners.Count -gt 0) {
            if (-not (Test-TraceDashboard)) {
                throw "端口 $dashboardPort 已被其他程序占用；为避免误操作，没有启动管理页。"
            }
            Write-Host "管理页已在运行：$dashboardUrl/"
        } else {
            $dashboardProcess = Start-Process -FilePath $python `
                -ArgumentList @('"' + $dashboardScript + '"') `
                -WorkingDirectory $root -WindowStyle Hidden `
                -RedirectStandardOutput (Join-Path $tempRoot 'dashboard.stdout.log') `
                -RedirectStandardError (Join-Path $tempRoot 'dashboard.stderr.log') `
                -PassThru
            $dashboardReady = $false
            for ($attempt = 0; $attempt -lt 30; $attempt++) {
                Start-Sleep -Milliseconds 500
                if (Test-TraceDashboard) {
                    $dashboardReady = $true
                    break
                }
                if ($dashboardProcess.HasExited) { break }
            }
            if (-not $dashboardReady) {
                throw '管理页启动失败；请查看 .tmp\dashboard.stderr.log。'
            }
            Write-Host "管理页已启动：$dashboardUrl/"
        }

        # Start Scheme A Agent Ingress (:8080) + Worker before waiting on WeChat login so
        # initial login-sync callbacks do not bounce on a closed port.
        & $liveScript -Action start | Out-Null

        if ($customGateway) {
            $activeLabel = if ($configuredFactory) { $configuredFactory } elseif ($runtimeMeta -and $runtimeMeta.sources) { ($runtimeMeta.sources -join ', ') } else { 'custom_connector' }
            Write-Host ('使用配置的信息源连接器：' + $activeLabel)
            if ($env:WECHAT_HOOK_URL) { Write-Host ('交互接口：' + $env:WECHAT_HOOK_URL) }
        } else {
            & $hookPluginScript -LoginWaitSeconds $LoginWaitSeconds -Root $root -HookConfig $hookCfg
        }

        & $liveScript -Action start | Out-Null
        if (-not $customGateway) {
            for ($wait = 0; $wait -lt 10; $wait++) {
                try {
                    $postStart = Invoke-RestMethod $agentStatusUrl -TimeoutSec 2
                    if ($postStart.mode -eq 'selfhosted_group' -and [bool]$postStart.worker_alive -and [bool]$postStart.trusted_gateway) {
                        break
                    }
                } catch {}
                Start-Sleep -Seconds 1
            }
        }
    }

    $healthy = Invoke-HealthCheck $customGateway
    if (-not $healthy) {
        throw '部分服务健康检查未通过，请查看上方 [FAIL] 项与 .tmp\ 下的日志。'
    }

    Write-Host ''
    Write-Host '全部服务健康且已就绪：'
    if (-not $customGateway) {
        Write-Host '  微信 Hook：     http://127.0.0.1:30001/QueryDB/status'
        Write-Host '  原图下载扩展：  http://127.0.0.1:30002/status'
        Write-Host '  长卡片接收扩展：http://127.0.0.1:30003/status'
    }
    Write-Host '  Agent Pilot：   http://127.0.0.1:8080/status'
    Write-Host '  管理页面：      http://127.0.0.1:8767/'
} catch {
    $exitCode = 1
    Write-Host ("`n[ERROR] {0}" -f $_.Exception.Message) -ForegroundColor Red
} finally {
    Write-Host ("[{0}] 结束（exit={1}）" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $exitCode)
    if ($transcriptStarted) {
        try { Stop-Transcript | Out-Null } catch {}
    }
}
if ($exitCode -ne 0) { exit $exitCode }
