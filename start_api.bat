@echo off
echo Starting Biosphere Pipeline API Server...
echo.

REM Check if virtual environment exists
if not exist "venv\Scripts\activate.bat" (
    echo Virtual environment not found. Please run setup first.
    echo.
    echo To set up the environment:
    echo 1. python -m venv venv
    echo 2. venv\Scripts\activate
    echo 3. pip install -r requirements_api.txt
    pause
    exit /b 1
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Check if API dependencies are installed
echo Checking API dependencies...
python -c "import fastapi, uvicorn" 2>nul
if errorlevel 1 (
    echo Installing API dependencies...
    pip install -r requirements_api.txt
    if errorlevel 1 (
        echo Failed to install dependencies.
        pause
        exit /b 1
    )
)

REM Start the API server
echo.
echo Starting API server on http://localhost:8000
echo Press Ctrl+C to stop the server
echo.
echo API Documentation will be available at:
echo   - Swagger UI: http://localhost:8000/docs
echo   - ReDoc: http://localhost:8000/redoc
echo.

python scripts\api_server.py

pause
