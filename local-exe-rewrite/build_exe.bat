@echo on
setlocal
cd /d %~dp0

set "PY_CMD="
py -3.11 -V >nul 2>&1 && set "PY_CMD=py -3.11"
if not defined PY_CMD (
  py -V >nul 2>&1 && set "PY_CMD=py"
)
if not defined PY_CMD (
  python -V >nul 2>&1 && set "PY_CMD=python"
)
if not defined PY_CMD goto :no_python

echo Using interpreter: %PY_CMD%

if not exist .venv (
  %PY_CMD% -m venv .venv
  if errorlevel 1 goto :fail
)

call .venv\Scripts\activate.bat
if errorlevel 1 goto :fail

python -m pip install -U pip
if errorlevel 1 goto :fail

python -m pip install -r requirements.txt
if errorlevel 1 goto :fail

python -m pip install pyinstaller
if errorlevel 1 goto :fail

pyinstaller --noconfirm --clean --name RefundAuditLocal --windowed app\main.py
if errorlevel 1 goto :fail

echo.
echo Build succeeded.
echo EXE path: %~dp0dist\RefundAuditLocal\RefundAuditLocal.exe
goto :end

:no_python
echo.
echo Build failed: no Python interpreter found.
echo Please install Python 3.11+ or run: py install 3.11
goto :end

:fail
echo.
echo Build failed with error code %errorlevel%.

:end
echo.
echo Press any key to close this window.
pause >nul
endlocal
