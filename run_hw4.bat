@echo off
setlocal

cd /d "%~dp0"

if not exist ".env" (
  echo [INFO] .env not found. Creating it from .env.example...
  copy /Y ".env.example" ".env" >nul
  echo [ACTION REQUIRED] Update .env with your MARKETCHECK_API_KEY, then run this file again.
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo [INFO] Creating virtual environment...
  where py >nul 2>nul
  if %errorlevel%==0 (
    py -3 -m venv .venv
  ) else (
    python -m venv .venv
  )
  if %errorlevel% neq 0 goto :error
)

echo [INFO] Installing/updating dependencies...
".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -r requirements.txt
if %errorlevel% neq 0 goto :error

echo [INFO] Opening browser...
start "" "http://127.0.0.1:8000/"

echo [INFO] Starting server at http://127.0.0.1:8000
echo [INFO] Press Ctrl+C to stop.
".venv\Scripts\python.exe" -m uvicorn app.main:app --reload
goto :eof

:error
echo [ERROR] Startup failed. Fix the issue above and run run_hw4.bat again.
pause
exit /b 1

