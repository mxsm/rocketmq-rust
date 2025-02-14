@echo off
setlocal

:: Save the current directory
set CURRENT_DIR=%cd%

:: Navigate to the workspace root directory
cd ..

echo Starting to package Rust workspace projects...

:: 分行定义 PROJECTS 变量
set "PROJECTS=rocketmq-common ^
rocketmq-runtime ^
rocketmq-macros ^
rocketmq ^
rocketmq-filter ^
rocketmq-store ^
rocketmq-remoting ^
rocketmq-cli ^
rocketmq-example ^
rocketmq-client ^
rocketmq-namesrv ^
rocketmq-broker ^
rocketmq-tools ^
rocketmq-tui"

for %%P in (%PROJECTS%) do (
    echo Packaging %%P...
    cd %%P
    cargo package
    cargo publish
    if %errorlevel% neq 0 (
        echo %%P packaging failed.
        cd %CURRENT_DIR%
        exit /b %errorlevel%
    )
    cd ..
)

echo Finished packaging projects.
:: Return to the original directory
cd %CURRENT_DIR%

endlocal