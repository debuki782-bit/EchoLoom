$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
& (Join-Path $root '.venv\Scripts\python.exe') -m uvicorn wechat_agent.app:app --host 127.0.0.1 --port 8765
