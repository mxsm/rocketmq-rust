@echo off
setlocal EnableExtensions

python "%~dp0package_publish_workspace.py" %*
exit /b %errorlevel%
