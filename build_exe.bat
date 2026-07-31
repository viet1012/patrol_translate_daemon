@echo off
setlocal EnableExtensions

cd /d "%~dp0"

set "VENV_PY=%CD%\.venv\Scripts\python.exe"
set "APP_NAME=PatrolTranslate"
set "ENTRY_FILE=patrol_translate_ui.py"

echo ==========================================
echo BUILD PATROL TRANSLATE
echo ==========================================
echo Project: %CD%
echo.

REM ==========================================================
REM 1. Kiem tra Python
REM ==========================================================

where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Khong tim thay Python trong PATH.
    echo Hay cai Python 3.11 hoac 3.12 x64.
    goto :error
)

REM ==========================================================
REM 2. Tao virtual environment
REM ==========================================================

if not exist "%VENV_PY%" (
    echo [1/7] Tao virtual environment...

    python -m venv ".venv"
    if errorlevel 1 (
        echo [ERROR] Khong tao duoc virtual environment.
        goto :error
    )
) else (
    echo [1/7] Virtual environment da ton tai.
)

REM ==========================================================
REM 3. Nang cap pip
REM ==========================================================

echo [2/7] Nang cap pip...

"%VENV_PY%" -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
    echo [ERROR] Khong nang cap duoc pip.
    goto :error
)

REM ==========================================================
REM 4. Cai dependencies
REM ==========================================================

echo [3/7] Cai dependencies...

"%VENV_PY%" -m pip install -r "requirements.txt"
if errorlevel 1 (
    echo [ERROR] Cai requirements.txt that bai.
    goto :error
)

"%VENV_PY%" -m pip install --upgrade pyinstaller
if errorlevel 1 (
    echo [ERROR] Cai PyInstaller that bai.
    goto :error
)

REM ==========================================================
REM 5. Kiem tra import truoc khi build
REM ==========================================================

echo [4/7] Kiem tra cac thu vien...

"%VENV_PY%" -c "import requests; import pyodbc; import dotenv; print('requests:', requests.__version__); print('pyodbc:', pyodbc.version); print('Dependencies OK')"
if errorlevel 1 (
    echo.
    echo [ERROR] Moi truong build dang thieu dependency.
    echo Kiem tra lai requirements.txt.
    goto :error
)

echo.
echo Python build:
"%VENV_PY%" -c "import sys; print(sys.executable)"

echo.
echo PyInstaller:
"%VENV_PY%" -m PyInstaller --version
if errorlevel 1 goto :error

REM ==========================================================
REM 6. Xoa build cu
REM ==========================================================

echo.
echo [5/7] Xoa build cu...

if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"
if exist "release" rmdir /s /q "release"
if exist "%APP_NAME%.spec" del /f /q "%APP_NAME%.spec"

REM ==========================================================
REM 7. Build bang dung Python trong .venv
REM ==========================================================

echo [6/7] Build EXE...

"%VENV_PY%" -m PyInstaller ^
    --noconfirm ^
    --clean ^
    --windowed ^
    --onedir ^
    --name "%APP_NAME%" ^
    --hidden-import=requests ^
    --hidden-import=urllib3 ^
    --hidden-import=certifi ^
    --hidden-import=charset_normalizer ^
    --hidden-import=idna ^
    --hidden-import=pyodbc ^
    --hidden-import=dotenv ^
    --collect-all=requests ^
    --collect-all=urllib3 ^
    --collect-all=certifi ^
    --collect-all=charset_normalizer ^
    --collect-all=pyodbc ^
    "%ENTRY_FILE%"

if errorlevel 1 (
    echo [ERROR] PyInstaller build that bai.
    goto :error
)

REM ==========================================================
REM 8. Tao release
REM ==========================================================

echo [7/7] Tao thu muc release...

mkdir "release" >nul 2>&1

xcopy ^
    "dist\%APP_NAME%" ^
    "release\%APP_NAME%\" ^
    /E /I /H /Y

if errorlevel 1 (
    echo [ERROR] Khong copy duoc thu muc build vao release.
    goto :error
)

if exist ".env" (
    copy /Y ".env" "release\%APP_NAME%\.env" >nul
) else (
    echo [WARNING] Khong tim thay file .env.
)

REM Tao cac thu muc runtime rong
if not exist "release\%APP_NAME%\logs" (
    mkdir "release\%APP_NAME%\logs"
)

REM Khong copy cache, lock, log, state cu
del /F /Q "release\%APP_NAME%\patrol_translate_cache.sqlite3" >nul 2>&1
del /F /Q "release\%APP_NAME%\patrol_translate_cache_ui.sqlite3" >nul 2>&1
del /F /Q "release\%APP_NAME%\patrol_translate_state.json" >nul 2>&1
del /F /Q "release\%APP_NAME%\patrol_translate.lock" >nul 2>&1
del /F /Q "release\%APP_NAME%\lm_studio_reload.lock" >nul 2>&1

echo.
echo ==========================================
echo BUILD THANH CONG
echo ==========================================
echo.
echo File chay:
echo %CD%\release\%APP_NAME%\%APP_NAME%.exe
echo.
echo Hay chay thu file EXE truoc khi tao installer.
echo ==========================================

pause
exit /b 0


:error
echo.
echo ==========================================
echo BUILD THAT BAI
echo ==========================================
echo Kiem tra cac dong loi o phia tren.
echo.
pause
exit /b 1