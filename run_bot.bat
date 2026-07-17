@echo off
chcp 65001 >nul
title DOC BOT
cd /d "%~dp0"

echo ============================================
echo   DOC BOT - launching
echo   Folder: %CD%
echo ============================================
echo.

rem --- Stop old bot.py instances (avoid Telegram Conflict) ---
echo [*] Checking for old bot processes...
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe' OR Name='pythonw.exe'\" | Where-Object { $_.CommandLine -like '*bot.py*' } | ForEach-Object { Write-Host ('[*] Stopping old PID ' + $_.ProcessId); Stop-Process -Id $_.ProcessId -Force }"
echo.

rem --- Find python ---
set "PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
if not exist "%PY%" set "PY=python"

:run
echo [*] Starting bot...
echo --------------------------------------------
"%PY%" bot.py
set "EXITCODE=%ERRORLEVEL%"

echo --------------------------------------------
rem Exit code 0 = clean stop (Ctrl+C) - do NOT restart.
if "%EXITCODE%"=="0" goto stopped

echo [!] Bot crashed (exit code %EXITCODE%). Restarting in 10 seconds...
echo     Close this window to cancel auto-restart.
timeout /t 10 /nobreak >nul
goto run

:stopped
echo [!] Bot stopped cleanly (exit code 0).
echo Press any key to close this window.
pause >nul
