@echo off
echo 🎯 Self-Improvement Dashboard Setup
echo ====================================
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python not found. Please install Python 3.8 or higher.
    pause
    exit /b 1
)

echo ✓ Python found
echo.

REM Create virtual environment
if not exist "venv" (
    echo 📦 Creating virtual environment...
    python -m venv venv
    echo ✓ Virtual environment created
) else (
    echo ✓ Virtual environment already exists
)

echo.

REM Install dependencies
echo 📥 Installing dependencies...
venv\Scripts\pip install -q --upgrade pip
venv\Scripts\pip install -q -r requirements.txt

if errorlevel 1 (
    echo ❌ Failed to install dependencies
    pause
    exit /b 1
)

echo ✓ Dependencies installed successfully
echo.

REM Create .env if it doesn't exist
if not exist ".env" (
    echo ⚙️  Creating .env file...
    copy .env.example .env
    echo ✓ Created .env file (edit it to customize settings)
)

echo.
echo 🎉 Setup complete!
echo.
echo To start the app:
echo   venv\Scripts\python app\app.py
echo.
echo Then open your browser to: http://localhost:8050
echo.
pause
