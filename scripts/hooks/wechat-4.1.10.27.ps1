param(
    [ValidateRange(30, 900)]
    [int]$LoginWaitSeconds = 300,
    [Parameter(Mandatory)]
    [string]$Root,
    [Parameter(Mandatory)]
    [pscustomobject]$HookConfig
)

$ErrorActionPreference = 'Stop'

function Get-OptionalHookStatus([string]$url) {
    try {
        return Invoke-RestMethod $url -TimeoutSec 3
    } catch {
        return $null
    }
}

function Test-HookLoggedIn($status) {
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

$hookBaseUrl = if ($HookConfig.hook_url) { [string]$HookConfig.hook_url } else { 'http://127.0.0.1:30001' }
$hookStatusUrl = ($hookBaseUrl.TrimEnd('/') + '/QueryDB/status')
$hookPort = ([Uri]$hookBaseUrl).Port

$hookStatus = Get-OptionalHookStatus $hookStatusUrl
if ($null -eq $hookStatus) {
    $hookListeners = @(
        Get-NetTCPConnection -LocalPort $hookPort -State Listen -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -Unique
    )
    if ($hookListeners.Count -gt 0) {
        throw "端口 $hookPort 已被占用，但 Hook 状态接口不可读；没有启动或注入微信。"
    }

    $weixinExe = [string]$HookConfig.weixin_exe
    $launcherRel = [string]$HookConfig.launcher_exe
    $hookDllRel = [string]$HookConfig.hook_dll
    $expectedLauncherSha = [string]$HookConfig.launcher_sha256
    $expectedDllSha = [string]$HookConfig.hook_dll_sha256
    $expectedVersion = [string]$HookConfig.weixin_version

    if (-not $weixinExe -or -not $launcherRel -or -not $hookDllRel) {
        throw '请在 .local/runtime.toml 的 [wechat_hook] 中配置 weixin_exe、launcher_exe 和 hook_dll，或先手动启动 Hook。'
    }

    $launcher = if ([IO.Path]::IsPathRooted($launcherRel)) { $launcherRel } else { Join-Path $Root $launcherRel }
    $hookDll = if ([IO.Path]::IsPathRooted($hookDllRel)) { $hookDllRel } else { Join-Path $Root $hookDllRel }

    $weixinProcesses = @(Get-CimInstance Win32_Process -Filter "Name='Weixin.exe'" -ErrorAction Stop)
    $expectedExe = [IO.Path]::GetFullPath($weixinExe)
    $otherWeixin = @($weixinProcesses | Where-Object {
        -not $_.ExecutablePath -or [IO.Path]::GetFullPath($_.ExecutablePath) -ine $expectedExe
    })
    if ($otherWeixin.Count -gt 0) {
        throw '检测到另一安装路径的 Weixin.exe。请先关闭该微信实例，避免重复登录或注入错误进程。'
    }

    foreach ($path in @($launcher, $hookDll, $weixinExe)) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Hook 启动所需文件不存在：$path"
        }
    }
    if ($expectedLauncherSha -and (Get-FileHash -LiteralPath $launcher -Algorithm SHA256).Hash -ne $expectedLauncherSha) {
        throw 'Hook Launcher 哈希与 .local/runtime.toml 配置不符；已停止启动。'
    }
    if ($expectedDllSha -and (Get-FileHash -LiteralPath $hookDll -Algorithm SHA256).Hash -ne $expectedDllSha) {
        throw 'Hook DLL 哈希与 .local/runtime.toml 配置不符；已停止启动。'
    }
    $weixinFile = Get-Item -LiteralPath $weixinExe
    $weixinSignature = Get-AuthenticodeSignature -FilePath $weixinExe
    if (($expectedVersion -and $weixinFile.VersionInfo.ProductVersion -ne $expectedVersion) -or
        $weixinSignature.Status -ne 'Valid') {
        throw '微信版本或签名与 .local/runtime.toml 配置不符；已停止启动。'
    }

    Write-Host '正在通过 Hook Launcher 启动微信；如需登录，请在微信窗口完成登录。'
    $prevWeixinExe = $env:ECHOLOOM_WEIXIN_EXE
    $prevHookPort = $env:ECHOLOOM_HOOK_PORT
    try {
        $env:ECHOLOOM_WEIXIN_EXE = $expectedExe
        $env:ECHOLOOM_HOOK_PORT = [string]$hookPort
        $launch = Start-Process -FilePath $launcher -ArgumentList '--start' -Wait -PassThru
    } finally {
        if ($null -eq $prevWeixinExe) { Remove-Item Env:ECHOLOOM_WEIXIN_EXE -ErrorAction SilentlyContinue } else { $env:ECHOLOOM_WEIXIN_EXE = $prevWeixinExe }
        if ($null -eq $prevHookPort) { Remove-Item Env:ECHOLOOM_HOOK_PORT -ErrorAction SilentlyContinue } else { $env:ECHOLOOM_HOOK_PORT = $prevHookPort }
    }
    if ($launch.ExitCode -ne 0) {
        throw "Hook Launcher 启动失败，退出码：$($launch.ExitCode)"
    }
    $hookStatus = Get-OptionalHookStatus $hookStatusUrl
}

if (-not (Test-HookLoggedIn $hookStatus)) {
    Write-Host "等待 Hook 就绪并完成微信登录（最多 $LoginWaitSeconds 秒）……"
    $deadline = (Get-Date).AddSeconds($LoginWaitSeconds)
    do {
        Start-Sleep -Seconds 2
        $hookStatus = Get-OptionalHookStatus $hookStatusUrl
        if (Test-HookLoggedIn $hookStatus) { break }
    } while ((Get-Date) -lt $deadline)
}
if (-not (Test-HookLoggedIn $hookStatus)) {
    throw 'Hook 尚未就绪或微信仍未登录。保持微信窗口打开，登录后再运行本脚本。'
}
Write-Host 'Hook 已就绪，微信已登录。'

$scriptsDir = Split-Path -Parent $PSScriptRoot
$nativeDllRel = if ($HookConfig.native_download_dll) { [string]$HookConfig.native_download_dll } else { '.runtime/native-image-download/wx_image_download_411027.dll' }
$nativeDll = if ([IO.Path]::IsPathRooted($nativeDllRel)) { $nativeDllRel } else { Join-Path $Root $nativeDllRel }
if (Test-Path -LiteralPath $nativeDll -PathType Leaf) {
    Ensure-HookExtension (Join-Path $scriptsDir 'native-image-download.ps1') '图片主动下载扩展 (30002)' $false
}
Ensure-HookExtension (Join-Path $scriptsDir 'hook-event-extension.ps1') '长卡片/拍一拍接收扩展 (30003)' $true
