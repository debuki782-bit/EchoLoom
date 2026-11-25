@echo off
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\start-services.ps1"
set "result=%ERRORLEVEL%"
if not "%result%"=="0" (
  echo Startup did not complete. See the message above, fix it, and run this file again.
)
echo.
pause
exit /b %result%
