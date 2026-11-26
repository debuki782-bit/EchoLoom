<#
.SYNOPSIS
One-click builder for the EchoLoom WeChat PC 4.1.10.27 Hook stack (Windows x64).

.DESCRIPTION
Fetches the pinned upstream WeChat Hook repository directly from GitHub
(mosheng20205/WechatHook4.1.10.27 @ 713ec7d6f01d858e067a623c9bf2cda64d0d3e19),
applies EchoLoom's minimal hardening & @mention patch, compiles:
  1. wx_agent_hook.dll       (loopback 127.0.0.1:30001 + group atlist support)
  2. WeChatHookLoader.exe    (exact 4.1.10.27 check + headless --start + unique DLL name)
  3. wx_image_download_411027.dll (optional native CDN full-image downloader on :30002)
and optionally installs isolated Frida 17.26.1 runtime deps for :30002 / :30003 extensions.

No upstream Hook source code or WeChat binaries are bundled in EchoLoom.
#>
param(
    [string]$OutDir = '',
    [string]$UpstreamZip = '',
    [string]$UpstreamDir = '',
    [switch]$SkipNativeImage,
    [switch]$SkipFrida,
    [switch]$KeepBuildTree
)

$ErrorActionPreference = 'Stop'

$RepoUrl = 'https://github.com/mosheng20205/WechatHook4.1.10.27'
$PinnedCommit = '713ec7d6f01d858e067a623c9bf2cda64d0d3e19'
$PinnedZipUrl = "$RepoUrl/archive/$PinnedCommit.zip"
$PinnedZipSha256 = '6580544B5020CE41A2D7479B2EB0DD76057B8E2DC7FA4B9F87FCB3BB1CEB4189'
$ExpectedWeixinDllSha256 = 'F487E4F56930456A25FB0D7B058C77E2DCD364E268D49BDFFF4DC0E3BE26BA36'

$hookAssetDir = $PSScriptRoot
$repoRoot = (Resolve-Path (Join-Path $hookAssetDir '..\..')).Path
$patchDir = Join-Path $hookAssetDir 'patches'
$patchFiles = @(Get-ChildItem -LiteralPath $patchDir -Filter '*.patch' -File | Sort-Object Name)
$nativeCpp = Join-Path $hookAssetDir 'native_image_download.cpp'
$fridaReq = Join-Path $hookAssetDir 'requirements-frida.txt'

if (-not $OutDir) {
    $OutDir = Join-Path $repoRoot '.runtime'
} elseif (-not [IO.Path]::IsPathRooted($OutDir)) {
    $OutDir = Join-Path $repoRoot $OutDir
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$OutDir = (Resolve-Path -LiteralPath $OutDir).Path

function Find-VsTool([string]$findPattern, [string]$requires = '') {
    $vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path -LiteralPath $vswhere -PathType Leaf) {
        $args = @('-all', '-products', '*', '-latest')
        if ($requires) { $args += @('-requires', $requires) }
        $args += @('-find', $findPattern)
        $found = @(& $vswhere @args | Where-Object { $_ -and (Test-Path -LiteralPath $_ -PathType Leaf) })
        if ($found.Count -gt 0) { return $found[0] }
    }
    return $null
}

function Resolve-Python {
    foreach ($candidate in @(
        $env:ECHOLOOM_PYTHON,
        (Join-Path $repoRoot '.venv-openai\Scripts\python.exe'),
        (Join-Path $repoRoot '.venv\Scripts\python.exe')
    )) {
        if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) { return $candidate }
    }
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    return $null
}

$msbuild = Find-VsTool 'MSBuild\**\Bin\MSBuild.exe' 'Microsoft.Component.MSBuild'
$vcvars = Find-VsTool 'VC\Auxiliary\Build\vcvars64.bat'
if (-not $msbuild -or -not $vcvars) {
    throw @"
Visual Studio 2022 Build Tools (Desktop development with C++ / v143 toolset) is required.
Install via winget:
  winget install Microsoft.VisualStudio.2022.BuildTools --override "--wait --quiet --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
"@
}
$gitCmd = Get-Command git -ErrorAction SilentlyContinue
if (-not $gitCmd) { throw 'git is required on PATH to apply the upstream patch.' }

Write-Host "Using MSBuild : $msbuild"
Write-Host "Using vcvars64: $vcvars"
Write-Host "Output root   : $OutDir"

# 1. Prepare clean working copy of pinned upstream commit
$buildBase = Join-Path $OutDir 'wechat-hook-build'
$srcDir = Join-Path $buildBase 'src'
if (Test-Path -LiteralPath $srcDir) {
    Remove-Item -LiteralPath $srcDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $buildBase | Out-Null

if ($UpstreamDir) {
    $resolvedSrc = (Resolve-Path -LiteralPath $UpstreamDir).Path
    Write-Host "Copying upstream source from $resolvedSrc ..."
    Copy-Item -LiteralPath $resolvedSrc -Destination $srcDir -Recurse -Force
} else {
    $zipPath = $UpstreamZip
    if (-not $zipPath) {
        $zipPath = Join-Path $buildBase "mosheng-$($PinnedCommit.Substring(0,8)).zip"
        if (-not (Test-Path -LiteralPath $zipPath -PathType Leaf)) {
            Write-Host "Downloading pinned upstream archive: $PinnedZipUrl"
            try {
                Invoke-WebRequest -Uri $PinnedZipUrl -OutFile $zipPath -UseBasicParsing -TimeoutSec 120
            } catch {
                Write-Warning "Archive download failed ($($_.Exception.Message)); falling back to git clone..."
                $zipPath = ''
            }
        }
    }

    if ($zipPath) {
        $actualSha = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash
        if ($actualSha -ne $PinnedZipSha256) {
            throw "Upstream ZIP SHA-256 mismatch! Expected $PinnedZipSha256, got $actualSha"
        }
        $extractRoot = Join-Path $buildBase 'extracted'
        if (Test-Path -LiteralPath $extractRoot) { Remove-Item -LiteralPath $extractRoot -Recurse -Force }
        Expand-Archive -LiteralPath $zipPath -DestinationPath $extractRoot -Force
        $inner = Join-Path $extractRoot "WechatHook4.1.10.27-$PinnedCommit"
        if (-not (Test-Path -LiteralPath $inner -PathType Container)) {
            $firstDir = @(Get-ChildItem -LiteralPath $extractRoot -Directory) | Select-Object -First 1
            if (-not $firstDir) { throw 'Extracted upstream archive contains no source folder.' }
            $inner = $firstDir.FullName
        }
        Move-Item -LiteralPath $inner -Destination $srcDir
        Remove-Item -LiteralPath $extractRoot -Recurse -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "Cloning $RepoUrl.git ..."
        & $gitCmd.Source clone --no-checkout "$RepoUrl.git" $srcDir
        if ($LASTEXITCODE -ne 0) { throw "git clone failed with exit code $LASTEXITCODE" }
        & $gitCmd.Source -C $srcDir checkout --detach $PinnedCommit
        if ($LASTEXITCODE -ne 0) { throw "git checkout $PinnedCommit failed with exit code $LASTEXITCODE" }
    }
}

# 2. Apply EchoLoom patches (loopback 127.0.0.1, default callback, group atlist, SendFileMsg, hardened launcher)
Push-Location $srcDir
try {
    # Isolate extracted/copied source from any parent EchoLoom .git repo so
    # `git apply` never skips `diff --git` headers under ignored build dirs.
    if (-not (Test-Path -LiteralPath '.git')) {
        & $gitCmd.Source init -q
        if ($LASTEXITCODE -ne 0) { throw "git init failed with exit code $LASTEXITCODE" }
    }
    foreach ($pf in $patchFiles) {
        Write-Host "Applying patch: $($pf.Name)"
        $prevPref = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            $applyOut = & $gitCmd.Source apply --verbose --ignore-space-change --ignore-whitespace --whitespace=nowarn $pf.FullName 2>&1
            $applyExit = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $prevPref
        }
        $applyOut | ForEach-Object { Write-Host $_ }
        if ($applyExit -ne 0 -or (($applyOut | Out-String) -match 'Skipped patch')) {
            throw "git apply failed or skipped hunks on $($pf.Name) (exit=$applyExit)"
        }
    }
} finally {
    Pop-Location
}

# 3. Build base Hook DLL (x64 Release) and Launcher EXE (x64 Release)
$hookOutDir = Join-Path $OutDir 'wechat-hook'
New-Item -ItemType Directory -Force -Path $hookOutDir | Out-Null

Write-Host 'Building Hook DLL (x64_Version_dll.vcxproj, Release|x64)...'
& $msbuild (Join-Path $srcDir 'x64_Version_dll.vcxproj') /nologo /v:minimal /m /t:Build /p:Configuration=Release /p:Platform=x64
if ($LASTEXITCODE -ne 0) { throw "MSBuild x64_Version_dll.vcxproj failed ($LASTEXITCODE)" }

Write-Host 'Building Hook Launcher (launcher\WeChatHookLauncher.vcxproj, Release|x64)...'
& $msbuild (Join-Path $srcDir 'launcher\WeChatHookLauncher.vcxproj') /nologo /v:minimal /m /t:Build /p:Configuration=Release /p:Platform=x64
if ($LASTEXITCODE -ne 0) { throw "MSBuild WeChatHookLauncher.vcxproj failed ($LASTEXITCODE)" }

$builtDll = Join-Path $srcDir 'x64\Release\version.dll'
$builtLauncher = Join-Path $srcDir 'launcher\bin\WeChatHookLauncher.exe'
if (-not (Test-Path -LiteralPath $builtDll -PathType Leaf)) { throw "Built DLL missing: $builtDll" }
if (-not (Test-Path -LiteralPath $builtLauncher -PathType Leaf)) { throw "Built launcher missing: $builtLauncher" }

$targetHookDll = Join-Path $hookOutDir 'wx_agent_hook.dll'
$targetLauncher = Join-Path $hookOutDir 'WeChatHookLoader.exe'
Copy-Item -LiteralPath $builtDll -Destination $targetHookDll -Force
Copy-Item -LiteralPath $builtLauncher -Destination $targetLauncher -Force

$hookDllSha = (Get-FileHash -LiteralPath $targetHookDll -Algorithm SHA256).Hash
$launcherSha = (Get-FileHash -LiteralPath $targetLauncher -Algorithm SHA256).Hash

# 4. Build optional native original-image CDN downloader DLL (:30002)
$targetNativeDll = ''
$nativeDllSha = ''
if (-not $SkipNativeImage) {
    $nativeOutDir = Join-Path $OutDir 'native-image-download'
    New-Item -ItemType Directory -Force -Path $nativeOutDir | Out-Null
    $minhookInclude = Join-Path $srcDir 'minhook\include'
    $minhookSources = @('buffer.c', 'hook.c', 'trampoline.c', 'hde\hde64.c') | ForEach-Object {
        '"' + (Join-Path $srcDir "minhook\src\$_") + '"'
    }
    $buildCmd = Join-Path $nativeOutDir 'build.cmd'
    @(
        '@echo off',
        "call `"$vcvars`" >nul",
        'if errorlevel 1 exit /b 1',
        ('cl /nologo /LD /O2 /MD /EHsc /std:c++17 /utf-8 /W4 /I"' + $minhookInclude + '" "' + $nativeCpp + '" ' + ($minhookSources -join ' ') + ' /link /OUT:wx_image_download_411027.dll bcrypt.lib /MAP'),
        'exit /b %errorlevel%'
    ) | Set-Content -LiteralPath $buildCmd -Encoding ascii

    Write-Host 'Building native image download DLL (wx_image_download_411027.dll)...'
    Push-Location $nativeOutDir
    try {
        & $env:ComSpec /d /c $buildCmd
        if ($LASTEXITCODE -ne 0) { throw "Native image DLL build failed ($LASTEXITCODE)" }
    } finally {
        Pop-Location
    }
    $targetNativeDll = Join-Path $nativeOutDir 'wx_image_download_411027.dll'
    $nativeDllSha = (Get-FileHash -LiteralPath $targetNativeDll -Algorithm SHA256).Hash
}

# 5. Install isolated Frida 17.26.1 deps for :30002 / :30003 extensions
if (-not $SkipFrida) {
    $fridaDir = Join-Path $OutDir 'native-image-download-deps'
    if (Test-Path -LiteralPath (Join-Path $fridaDir 'frida\__init__.py') -PathType Leaf) {
        Write-Host "Isolated Frida deps already present at $fridaDir"
    } else {
        $py = Resolve-Python
        if (-not $py) {
            Write-Warning 'Python not found; skipping isolated Frida installation. Run: python -m pip install --target .runtime/native-image-download-deps -r hooks/wechat-4.1.10.27/requirements-frida.txt'
        } else {
            Write-Host "Installing isolated Frida deps into $fridaDir ..."
            & $py -m pip install --target $fridaDir -r $fridaReq
            if ($LASTEXITCODE -ne 0) { throw "pip install Frida failed ($LASTEXITCODE)" }
        }
    }
}

if (-not $KeepBuildTree) {
    Remove-Item -LiteralPath $srcDir -Recurse -Force -ErrorAction SilentlyContinue
}

function To-RelOrAbs([string]$p) {
    if ($p.StartsWith($repoRoot + '\', [System.StringComparison]::OrdinalIgnoreCase)) {
        return $p.Substring($repoRoot.Length + 1).Replace('\', '/')
    }
    return $p.Replace('\', '/')
}

Write-Host ''
Write-Host '================ Build Succeeded ================' -ForegroundColor Green
Write-Host ("Hook DLL        : {0} (SHA256={1})" -f $targetHookDll, $hookDllSha)
Write-Host ("Hook Launcher   : {0} (SHA256={1})" -f $targetLauncher, $launcherSha)
if ($targetNativeDll) {
    Write-Host ("Native Img DLL  : {0} (SHA256={1})" -f $targetNativeDll, $nativeDllSha)
}
Write-Host ''
Write-Host 'Paste into .local/runtime.toml:' -ForegroundColor Cyan
Write-Host '[wechat_hook]'
Write-Host 'hook_url = "http://127.0.0.1:30001"'
Write-Host 'native_download_url = "http://127.0.0.1:30002"'
Write-Host 'event_extension_url = "http://127.0.0.1:30003"'
Write-Host 'weixin_exe = "C:\\Program Files\\Tencent\\Weixin\\Weixin.exe"'
Write-Host ('launcher_exe = "' + (To-RelOrAbs $targetLauncher) + '"')
Write-Host ('launcher_sha256 = "' + $launcherSha + '"')
Write-Host ('hook_dll = "' + (To-RelOrAbs $targetHookDll) + '"')
Write-Host ('hook_dll_sha256 = "' + $hookDllSha + '"')
Write-Host 'weixin_version = "4.1.10.27"'
if ($targetNativeDll) {
    Write-Host ('native_download_dll = "' + (To-RelOrAbs $targetNativeDll) + '"')
}
Write-Host ('weixin_dll_sha256 = "' + $ExpectedWeixinDllSha256 + '"')
