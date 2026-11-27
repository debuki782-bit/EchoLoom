@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
title WeChat AI Agent - Stop Services
echo [stop-services] Stopping Agent, Dashboard, Extensions, and Service Monitor (keeping WeChat and Hook)...
echo.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\stop-services.ps1" %*
set "result=%ERRORLEVEL%"
echo.
if "%result%"=="0" (
  echo [stop-services] All agent services stopped cleanly.
) else (
  echo [stop-services] Some services failed to stop cleanly ^(exit=%result%^). See details above.
)
pause
exit /b %result%
