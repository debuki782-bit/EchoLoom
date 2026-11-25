$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$env:TEMP = Join-Path $root '.tmp'
$env:TMP = $env:TEMP
New-Item -ItemType Directory -Force -Path $env:TEMP | Out-Null
& (Join-Path $root '.venv-openai\Scripts\python.exe') -m pytest (Join-Path $root 'tests') -q --basetemp (Join-Path $root '.tmp\pytest-runtime')
