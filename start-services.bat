@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
title WeChat AI Agent - Service Monitor
echo [start-services] Starting all services, auto-heal monitor, and live message stream...
echo [start-services] Log file: "%~dp0.tmp\service-monitor.log"
echo [start-services] Press Ctrl+C to stop the monitor window (services stay running).
echo.
"%~dp0.venv-openai\Scripts\python.exe" -u "%~dp0scripts\watch-services.py" %*
set "result=%ERRORLEVEL%"
echo.
echo [start-services] Monitor exited ^(exit=%result%^). Log: "%~dp0.tmp\service-monitor.log"
pause
exit /b %result%
