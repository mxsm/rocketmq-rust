@echo off
setlocal EnableDelayedExpansion

:: Parse command line arguments
set DRY_RUN=0
set SKIP_PACKAGE=0
set SPECIFIC_PROJECT=
set ALLOW_DIRTY=0
set VERBOSE=0
set ALL_FEATURES=0
set NO_DEFAULT_FEATURES=0
set FEATURES=

:parse_args
if "%~1"=="" goto args_done
if /i "%~1"=="--dry-run" (
    set DRY_RUN=1
    shift
    goto parse_args
)
if /i "%~1"=="--skip-package" (
    set SKIP_PACKAGE=1
    shift
    goto parse_args
)
if /i "%~1"=="--allow-dirty" (
    set ALLOW_DIRTY=1
    shift
    goto parse_args
)
if /i "%~1"=="--verbose" (
    set VERBOSE=1
    shift
    goto parse_args
)
if /i "%~1"=="--all-features" (
    set ALL_FEATURES=1
    shift
    goto parse_args
)
if /i "%~1"=="--no-default-features" (
    set NO_DEFAULT_FEATURES=1
    shift
    goto parse_args
)
if /i "%~1"=="--features" (
    set FEATURES=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--project" (
    set SPECIFIC_PROJECT=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--help" (
    echo ========================================
    echo RocketMQ Rust Workspace Publisher
    echo ========================================
    echo.
    echo USAGE:
    echo   package_publish_workspace.bat [OPTIONS]
    echo.
    echo DESCRIPTION:
    echo   Automate packaging and publishing of all RocketMQ Rust workspace
    echo   crates to crates.io in correct dependency order.
    echo.
    echo OPTIONS:
    echo   --dry-run              Run cargo package without publishing
    echo   --skip-package         Skip cargo package step, only publish
    echo   --allow-dirty          Allow publishing with uncommitted changes
    echo   --project NAME         Only package/publish specific project
    echo   --verbose              Enable verbose output
    echo   --all-features         Activate all available features
    echo   --no-default-features  Do not activate default features
    echo   --features FEATURES    Space or comma separated list of features
    echo   --help                 Show this help message
    echo.
    echo EXAMPLES:
    echo   1. Test packaging without publishing:
    echo      package_publish_workspace.bat --dry-run
    echo.
    echo   2. Publish all projects with verbose output:
    echo      package_publish_workspace.bat --verbose
    echo.
    echo   3. Publish specific project only:
    echo      package_publish_workspace.bat --project rocketmq-common
    echo.
    echo   4. Publish with all features enabled:
    echo      package_publish_workspace.bat --all-features
    echo.
    echo   5. Publish with specific features:
    echo      package_publish_workspace.bat --features "local_file_store,async_fs"
    echo.
    echo   6. Allow dirty working directory ^(for testing^):
    echo      package_publish_workspace.bat --allow-dirty --dry-run
    echo.
    echo   7. Skip packaging, only publish:
    echo      package_publish_workspace.bat --skip-package
    echo.
    echo PUBLISH ORDER:
    echo   Projects are published in dependency order:
    echo   rocketmq-error ^> rocketmq-common ^> rocketmq-runtime ^>
    echo   rocketmq-macros ^> rocketmq ^> rocketmq-filter ^> rocketmq-store ^>
    echo   rocketmq-remoting ^> rocketmq-cli ^> rocketmq-client ^>
    echo   rocketmq-namesrv ^> rocketmq-broker ^> rocketmq-tools ^>
    echo   rocketmq-tui ^> rocketmq-example ^> rocketmq-controller
    echo.
    echo NOTES:
    echo   - Requires cargo login credentials configured
    echo   - Each project must have a valid Cargo.toml
    echo   - Working directory should be clean ^(unless --allow-dirty^)
    echo   - Script will exit on first failure
    echo.
    exit /b 0
)
shift
goto parse_args

:args_done

:: Save the current directory
set CURRENT_DIR=%cd%

:: Navigate to the workspace root directory
cd ..

echo ========================================
echo RocketMQ Rust Workspace Publisher
echo ========================================
echo Start Time: %date% %time%
if %DRY_RUN%==1 echo Mode: DRY RUN (no publishing)
if %SKIP_PACKAGE%==1 echo Mode: SKIP PACKAGE
if %ALLOW_DIRTY%==1 echo Mode: ALLOW DIRTY
if %VERBOSE%==1 echo Mode: VERBOSE OUTPUT
if %ALL_FEATURES%==1 echo Features: ALL FEATURES
if %NO_DEFAULT_FEATURES%==1 echo Features: NO DEFAULT FEATURES
if not "%FEATURES%"=="" echo Features: %FEATURES%
if not "%SPECIFIC_PROJECT%"=="" echo Target: %SPECIFIC_PROJECT% only
echo ========================================
echo.

set SUCCESS_COUNT=0
set FAIL_COUNT=0
set SKIP_COUNT=0

:: Define projects in dependency order
set PROJECTS=rocketmq-error rocketmq-common rocketmq-runtime rocketmq-macros rocketmq rocketmq-filter rocketmq-store rocketmq-remoting rocketmq-cli rocketmq-client rocketmq-namesrv rocketmq-broker rocketmq-tools rocketmq-tui rocketmq-example rocketmq-controller

for %%P in (%PROJECTS%) do (
    :: Skip if specific project is set and this is not it
    if not "%SPECIFIC_PROJECT%"=="" (
        if not "%%P"=="%SPECIFIC_PROJECT%" (
            set /a SKIP_COUNT+=1
            goto :continue
        )
    )

    echo.
    echo [%%P] Processing...
    
    if not exist %%P (
        echo [%%P] ERROR: Directory not found
        set /a FAIL_COUNT+=1
        goto :continue
    )
    
    cd %%P
    
    :: Run cargo package unless skipped
    if %SKIP_PACKAGE%==0 (
        echo [%%P] Running cargo package...
        set PACKAGE_CMD=cargo package
        if %ALLOW_DIRTY%==1 set PACKAGE_CMD=!PACKAGE_CMD! --allow-dirty
        if %VERBOSE%==1 set PACKAGE_CMD=!PACKAGE_CMD! --verbose
        if %ALL_FEATURES%==1 set PACKAGE_CMD=!PACKAGE_CMD! --all-features
        if %NO_DEFAULT_FEATURES%==1 set PACKAGE_CMD=!PACKAGE_CMD! --no-default-features
        if not "%FEATURES%"=="" set PACKAGE_CMD=!PACKAGE_CMD! --features "%FEATURES%"
        !PACKAGE_CMD!
        if !errorlevel! neq 0 (
            echo [%%P] ERROR: cargo package failed
            set /a FAIL_COUNT+=1
            cd ..
            goto :continue
        )
        echo [%%P] Package created successfully
    )
    
    :: Run cargo publish unless dry-run
    if %DRY_RUN%==0 (
        echo [%%P] Publishing to crates.io...
        set PUBLISH_CMD=cargo publish
        if %ALLOW_DIRTY%==1 set PUBLISH_CMD=!PUBLISH_CMD! --allow-dirty
        if %VERBOSE%==1 set PUBLISH_CMD=!PUBLISH_CMD! --verbose
        if %ALL_FEATURES%==1 set PUBLISH_CMD=!PUBLISH_CMD! --all-features
        if %NO_DEFAULT_FEATURES%==1 set PUBLISH_CMD=!PUBLISH_CMD! --no-default-features
        if not "%FEATURES%"=="" set PUBLISH_CMD=!PUBLISH_CMD! --features "%FEATURES%"
        !PUBLISH_CMD!
        if !errorlevel! neq 0 (
            echo [%%P] ERROR: cargo publish failed
            set /a FAIL_COUNT+=1
            cd ..
            goto :continue
        )
        echo [%%P] Published successfully
    ) else (
        echo [%%P] SKIPPED: dry-run mode enabled
    )
    
    set /a SUCCESS_COUNT+=1
    cd ..
    
    :continue
)

echo.
echo ========================================
echo Summary
echo ========================================
echo Success: !SUCCESS_COUNT!
echo Failed:  !FAIL_COUNT!
if !SKIP_COUNT! gtr 0 echo Skipped: !SKIP_COUNT!
echo End Time: %date% %time%
echo ========================================

:: Return to the original directory
cd %CURRENT_DIR%

if !FAIL_COUNT! gtr 0 (
    echo.
    echo ERROR: Some projects failed to publish
    exit /b 1
)

echo.
echo All projects processed successfully!
endlocal
exit /b 0