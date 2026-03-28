@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "DRY_RUN=0"
set "SKIP_PACKAGE=0"
set "SPECIFIC_PROJECT="
set "ALLOW_DIRTY=0"
set "VERBOSE=0"
set "ALL_FEATURES=0"
set "NO_DEFAULT_FEATURES=0"
set "FEATURES="
set "SUCCESS_COUNT=0"
set "FAIL_COUNT=0"
set "SKIP_COUNT=0"
set "MATCH_COUNT=0"

set "CURRENT_DIR=%cd%"
for %%I in ("%~dp0.") do set "SCRIPT_DIR=%%~fI"
for %%I in ("%SCRIPT_DIR%\..") do set "WORKSPACE_ROOT=%%~fI"

set "PROJECT_COUNT=16"
set "PROJECT_1_NAME=rocketmq-error"
set "PROJECT_1_PATH=rocketmq-error"
set "PROJECT_1_ALIASES=rocketmq-error"
set "PROJECT_2_NAME=rocketmq-macros"
set "PROJECT_2_PATH=rocketmq-macros"
set "PROJECT_2_ALIASES=rocketmq-macros"
set "PROJECT_3_NAME=rocketmq-runtime"
set "PROJECT_3_PATH=rocketmq-runtime"
set "PROJECT_3_ALIASES=rocketmq-runtime"
set "PROJECT_4_NAME=rocketmq-dashboard-common"
set "PROJECT_4_PATH=rocketmq-dashboard\rocketmq-dashboard-common"
set "PROJECT_4_ALIASES=rocketmq-dashboard-common"
set "PROJECT_5_NAME=rocketmq-admin-cli"
set "PROJECT_5_PATH=rocketmq-tools\rocketmq-admin\rocketmq-admin-cli"
set "PROJECT_5_ALIASES=rocketmq-admin-cli"
set "PROJECT_6_NAME=rocketmq-rust"
set "PROJECT_6_PATH=rocketmq"
set "PROJECT_6_ALIASES=rocketmq,rocketmq-rust"
set "PROJECT_7_NAME=rocketmq-admin-tui"
set "PROJECT_7_PATH=rocketmq-tools\rocketmq-admin\rocketmq-admin-tui"
set "PROJECT_7_ALIASES=rocketmq-admin-tui"
set "PROJECT_8_NAME=rocketmq-common"
set "PROJECT_8_PATH=rocketmq-common"
set "PROJECT_8_ALIASES=rocketmq-common"
set "PROJECT_9_NAME=rocketmq-filter"
set "PROJECT_9_PATH=rocketmq-filter"
set "PROJECT_9_ALIASES=rocketmq-filter"
set "PROJECT_10_NAME=rocketmq-remoting"
set "PROJECT_10_PATH=rocketmq-remoting"
set "PROJECT_10_ALIASES=rocketmq-remoting"
set "PROJECT_11_NAME=rocketmq-auth"
set "PROJECT_11_PATH=rocketmq-auth"
set "PROJECT_11_ALIASES=rocketmq-auth"
set "PROJECT_12_NAME=rocketmq-client-rust"
set "PROJECT_12_PATH=rocketmq-client"
set "PROJECT_12_ALIASES=rocketmq-client,rocketmq-client-rust"
set "PROJECT_13_NAME=rocketmq-namesrv"
set "PROJECT_13_PATH=rocketmq-namesrv"
set "PROJECT_13_ALIASES=rocketmq-namesrv"
set "PROJECT_14_NAME=rocketmq-store"
set "PROJECT_14_PATH=rocketmq-store"
set "PROJECT_14_ALIASES=rocketmq-store"
set "PROJECT_15_NAME=rocketmq-admin-core"
set "PROJECT_15_PATH=rocketmq-tools\rocketmq-admin\rocketmq-admin-core"
set "PROJECT_15_ALIASES=rocketmq-admin-core"
set "PROJECT_16_NAME=rocketmq-store-inspect"
set "PROJECT_16_PATH=rocketmq-tools\rocketmq-store-inspect"
set "PROJECT_16_ALIASES=rocketmq-store-inspect"

:parse_args
if "%~1"=="" goto args_done

if /i "%~1"=="--dry-run"              set "DRY_RUN=1"
if /i "%~1"=="--skip-package"         set "SKIP_PACKAGE=1"
if /i "%~1"=="--allow-dirty"          set "ALLOW_DIRTY=1"
if /i "%~1"=="--verbose"              set "VERBOSE=1"
if /i "%~1"=="--all-features"         set "ALL_FEATURES=1"
if /i "%~1"=="--no-default-features"  set "NO_DEFAULT_FEATURES=1"

if /i "%~1"=="--features" (
    set "FEATURES=%~2"
    shift
)

if /i "%~1"=="--project" (
    set "SPECIFIC_PROJECT=%~2"
    shift
)

if /i "%~1"=="--help" (
    echo Usage: package_publish_workspace.bat [OPTIONS]
    echo.
    echo Package and publish the current RocketMQ Rust workspace crates to crates.io
    echo in dependency order. Standalone projects are excluded from this workflow.
    echo.
    echo Options:
    echo   --dry-run
    echo   --skip-package
    echo   --allow-dirty
    echo   --project NAME
    echo   --verbose
    echo   --all-features
    echo   --no-default-features
    echo   --features "a,b,c"
    echo   --help
    echo.
    echo Package aliases:
    echo   rocketmq           ^(package: rocketmq-rust^)
    echo   rocketmq-client    ^(package: rocketmq-client-rust^)
    echo.
    echo Standalone projects excluded:
    echo   rocketmq-example
    echo   rocketmq-dashboard\rocketmq-dashboard-gpui
    echo   rocketmq-dashboard\rocketmq-dashboard-tauri\src-tauri
    echo Default release temporarily excludes:
    echo   rocketmq-controller
    echo   rocketmq-broker
    echo   rocketmq-proxy
    exit /b 0
)

shift
goto parse_args

:args_done
if not exist "%WORKSPACE_ROOT%\Cargo.toml" (
    echo [ERROR] Workspace root Cargo.toml not found: %WORKSPACE_ROOT%
    exit /b 1
)

cd /d "%WORKSPACE_ROOT%"

echo =====================================================
echo RocketMQ Rust Workspace Publisher
echo =====================================================
echo Workspace Root: %WORKSPACE_ROOT%
echo Start Time: %date% %time%
echo Standalone projects are excluded from publishing
if %DRY_RUN%==1                echo Mode: DRY RUN
if %SKIP_PACKAGE%==1           echo Mode: SKIP PACKAGE
if %ALLOW_DIRTY%==1            echo Mode: ALLOW DIRTY
if %VERBOSE%==1                echo Mode: VERBOSE
if %ALL_FEATURES%==1           echo Features: ALL FEATURES
if %NO_DEFAULT_FEATURES%==1    echo Features: NO DEFAULT FEATURES
if not "%FEATURES%"==""        echo Features: %FEATURES%
if not "%SPECIFIC_PROJECT%"=="" echo Target: %SPECIFIC_PROJECT%
echo =====================================================
echo.

for /L %%I in (1,1,%PROJECT_COUNT%) do (
    call :process_project %%I
)

echo.
echo =====================================================
echo Summary
echo =====================================================
echo Success: !SUCCESS_COUNT!
echo Failed:  !FAIL_COUNT!
echo Skipped: !SKIP_COUNT!
echo End Time: %date% %time%
echo =====================================================

cd /d "%CURRENT_DIR%"

if not "%SPECIFIC_PROJECT%"=="" if !MATCH_COUNT! equ 0 (
    echo [ERROR] No package matched --project "%SPECIFIC_PROJECT%"
    exit /b 1
)

if !FAIL_COUNT! gtr 0 exit /b 1

echo.
echo All packages processed successfully!
exit /b 0

:process_project
set "IDX=%~1"
call set "PROJECT=%%PROJECT_%IDX%_NAME%%"
call set "PROJECT_PATH=%%PROJECT_%IDX%_PATH%%"
call set "PROJECT_ALIASES=%%PROJECT_%IDX%_ALIASES%%"

call :matches_project "%SPECIFIC_PROJECT%" "!PROJECT!" "!PROJECT_PATH!" "!PROJECT_ALIASES!"
if errorlevel 1 (
    set /a SKIP_COUNT+=1
    goto :EOF
)

set /a MATCH_COUNT+=1

echo.
echo [!PROJECT!] Processing from !PROJECT_PATH!...

if not exist "%WORKSPACE_ROOT%\!PROJECT_PATH!" (
    echo [!PROJECT!] ERROR: Directory not found: !PROJECT_PATH!
    set /a FAIL_COUNT+=1
    goto :EOF
)

pushd "%WORKSPACE_ROOT%\!PROJECT_PATH!" >nul
set "DO_PUBLISH=1"

if %SKIP_PACKAGE%==0 (
    call :run_package !PROJECT!
    if !errorlevel! neq 0 (
        set "DO_PUBLISH=0"
    )
)

if %DRY_RUN%==0 (
    if !DO_PUBLISH!==1 (
        call :run_publish !PROJECT!
        if !errorlevel! neq 0 (
            set /a FAIL_COUNT+=1
        ) else (
            set /a SUCCESS_COUNT+=1
        )
    ) else (
        echo [!PROJECT!] SKIPPED: package failed
        set /a FAIL_COUNT+=1
    )
) else (
    echo [!PROJECT!] SKIPPED due to dry-run
    set /a SUCCESS_COUNT+=1
)

popd >nul
goto :EOF

:matches_project
set "FILTER=%~1"
set "NAME=%~2"
set "PATH_VALUE=%~3"
set "ALIASES=%~4"

if "%FILTER%"=="" exit /b 0
if /i "%FILTER%"=="%NAME%" exit /b 0
if /i "%FILTER%"=="%PATH_VALUE%" exit /b 0
for %%A in (%ALIASES:,= %) do (
    if /i "%FILTER%"=="%%~A" exit /b 0
)
exit /b 1

:run_package
set "P=%~1"
echo [!P!] Running cargo package...

set "CMD=cargo package"
if %ALLOW_DIRTY%==1         set "CMD=!CMD! --allow-dirty"
if %VERBOSE%==1             set "CMD=!CMD! --verbose"
if %ALL_FEATURES%==1        set "CMD=!CMD! --all-features"
if %NO_DEFAULT_FEATURES%==1 set "CMD=!CMD! --no-default-features"
if not "%FEATURES%"==""     set "CMD=!CMD! --features ""%FEATURES%"""

!CMD!
if !errorlevel! neq 0 (
    echo [!P!] ERROR: cargo package failed
    exit /b 1
)

echo [!P!] Package OK
exit /b 0

:run_publish
set "P=%~1"
echo [!P!] Publishing...

set "CMD=cargo publish"
if %ALLOW_DIRTY%==1         set "CMD=!CMD! --allow-dirty"
if %VERBOSE%==1             set "CMD=!CMD! --verbose"
if %ALL_FEATURES%==1        set "CMD=!CMD! --all-features"
if %NO_DEFAULT_FEATURES%==1 set "CMD=!CMD! --no-default-features"
if not "%FEATURES%"==""     set "CMD=!CMD! --features ""%FEATURES%"""

!CMD!
if !errorlevel! neq 0 (
    echo [!P!] ERROR: cargo publish failed
    exit /b 1
)

echo [!P!] Publish OK
exit /b 0
