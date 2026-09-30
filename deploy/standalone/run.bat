@echo off
setlocal enabledelayedexpansion

:: Go to repository root
cd /d "%~dp0\..\.."

echo === Consul Aggregator Standalone Runner ===

:: 1. Create virtual environment if it doesn't exist
if not exist "venv\" (
    echo =^> Creating virtual environment in .\venv...
    python -m venv venv
)

:: 2. Activate virtual environment
call venv\Scripts\activate.bat

:: 3. Install dependencies
echo =^> Installing dependencies...
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r requirements.txt

:: 4. Create default config if missing
if not exist ".env" (
    if exist ".env.template" (
        echo =^> No .env found. Creating one from .env.template...
        copy .env.template .env >nul
        echo    ^(You might want to stop the script and edit .env to suit your needs^)
        timeout /t 2 /nobreak >nul
    ) else (
        echo Error: Neither .env nor .env.template exist.
        exit /b 1
    )
)

:: 5. Export variables from .env
echo =^> Loading configuration from .env...
for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
    set "line=%%A"
    :: Ignore comments and empty lines
    if not "!line:~0,1!"=="#" if not "!line!"=="" (
        set "%%A=%%B"
    )
)

:: 6. Run the agent
echo =^> Starting Consul Aggregator (Mode: %MODE%)...
if "%API_PORT%"=="" set API_PORT=8099
echo    Dashboard should be accessible at http://localhost:%API_PORT% ^(if enabled^)
echo ---------------------------------------------------

python -m consul_aggregator
