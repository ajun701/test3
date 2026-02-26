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

python -m app.main
if errorlevel 1 goto :fail

goto :end

:no_python
echo.
echo Run failed: no Python interpreter found.
echo Please install Python 3.11+ or run: py install 3.11
goto :end

:fail
echo.
echo Run failed with error code %errorlevel%.

:end
echo.
echo Press any key to close this window.
pause >nul
endlocal
