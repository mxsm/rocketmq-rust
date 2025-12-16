@echo off
setlocal EnableDelayedExpansion

:: ------------------------------
:: Parse command line arguments
:: ------------------------------
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

if /i "%~1"=="--dry-run"              set DRY_RUN=1
if /i "%~1"=="--skip-package"         set SKIP_PACKAGE=1
if /i "%~1"=="--allow-dirty"          set ALLOW_DIRTY=1
if /i "%~1"=="--verbose"              set VERBOSE=1
if /i "%~1"=="--all-features"         set ALL_FEATURES=1
if /i "%~1"=="--no-default-features"  set NO_DEFAULT_FEATURES=1

if /i "%~1"=="--features" (
    set FEATURES=%~2
    shift
)

if /i "%~1"=="--project" (
    set SPECIFIC_PROJECT=%~2
    shift
)

if /i "%~1"=="--help" (
    call :print_help
    exit /b 0
)

shift
goto parse_args

:args_done


:: ------------------------------
:: Print summary banner
:: ------------------------------
set CURRENT_DIR=%cd%
cd ..

echo =====================================================
echo RocketMQ Rust Workspace Publisher
echo =====================================================
echo Start Time: %date% %time%
if %DRY_RUN%==1           echo Mode: DRY RUN
if %SKIP_PACKAGE%==1      echo Mode: SKIP PACKAGE
if %ALLOW_DIRTY%==1       echo Mode: ALLOW DIRTY
if %VERBOSE%==1           echo Mode: VERBOSE
if %ALL_FEATURES%==1      echo Features: ALL FEATURES
if %NO_DEFAULT_FEATURES%==1 echo Features: NO DEFAULT FEATURES
if not "%FEATURES%"==""   echo Features: %FEATURES%
if not "%SPECIFIC_PROJECT%"=="" echo Target: %SPECIFIC_PROJECT%
echo =====================================================
echo.


:: ------------------------------
:: Global counters
:: ------------------------------
set SUCCESS_COUNT=0
set FAIL_COUNT=0
set SKIP_COUNT=0


:: ------------------------------
:: Workspace crates (dependency order)
:: ------------------------------
set PROJECTS= ^
rocketmq-error ^
rocketmq-common ^
rocketmq-runtime ^
rocketmq-macros ^
rocketmq ^
rocketmq-filter ^
rocketmq-remoting ^
rocketmq-store ^
rocketmq-cli ^
rocketmq-client ^
rocketmq-namesrv ^
rocketmq-broker ^
rocketmq-tools ^
rocketmq-tui ^
rocketmq-example ^
rocketmq-controller


:: ------------------------------
:: Main loop
:: ------------------------------
for %%P in (%PROJECTS%) do (
    call :process_project %%P
)


:: ------------------------------
:: Final Summary
:: ------------------------------
echo.
echo =====================================================
echo Summary
echo =====================================================
echo Success: !SUCCESS_COUNT!
echo Failed:  !FAIL_COUNT!
echo Skipped: !SKIP_COUNT!
echo End Time: %date% %time%
echo =====================================================

cd "%CURRENT_DIR%"

if !FAIL_COUNT! gtr 0 exit /b 1

echo.
echo All projects processed successfully!
exit /b 0



:: =====================================================
:: Function: Process single project
:: =====================================================
:process_project
set PROJECT=%1

:: Skip if filtering by --project
if not "%SPECIFIC_PROJECT%"=="" (
    if not "%PROJECT%"=="%SPECIFIC_PROJECT%" (
        set /a SKIP_COUNT+=1
        goto :EOF
    )
)

echo.
echo [!PROJECT!] Processing...

:: Check directory exists
if not exist "!PROJECT!" (
    echo [!PROJECT!] ERROR: Directory not found
    set /a FAIL_COUNT+=1
    goto :EOF
)

cd "!PROJECT!"
set DO_PUBLISH=1


:: ------------------------------
:: Step 1: cargo package
:: ------------------------------
if %SKIP_PACKAGE%==0 (
    call :run_package !PROJECT!
    if !errorlevel! neq 0 (
        set DO_PUBLISH=0
    )
)


:: ------------------------------
:: Step 2: cargo publish
:: ------------------------------
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

cd ..
goto :EOF



:: =====================================================
:: Function: run_package
:: =====================================================
:run_package
set P=%1
echo [!P!] Running cargo package...

set CMD=cargo package
if %ALLOW_DIRTY%==1         set CMD=!CMD! --allow-dirty
if %VERBOSE%==1             set CMD=!CMD! --verbose
if %ALL_FEATURES%==1        set CMD=!CMD! --all-features
if %NO_DEFAULT_FEATURES%==1 set CMD=!CMD! --no-default-features
if not "%FEATURES%"==""     set CMD=!CMD! --features "%FEATURES%"

!CMD!
if !errorlevel! neq 0 (
    echo [!P!] ERROR: cargo package failed
    exit /b 1
)

echo [!P!] Package OK
exit /b 0



:: =====================================================
:: Function: run_publish
:: =====================================================
:run_publish
set P=%1
echo [!P!] Publishing...

set CMD=cargo publish
if %ALLOW_DIRTY%==1         set CMD=!CMD! --allow-dirty
if %VERBOSE%==1             set CMD=!CMD! --verbose
if %ALL_FEATURES%==1        set CMD=!CMD! --all-features
if %NO_DEFAULT_FEATURES%==1 set CMD=!CMD! --no-default-features
if not "%FEATURES%"==""     set CMD=!CMD! --features "%FEATURES%"

!CMD!
if !errorlevel! neq 0 (
    echo [!P!] ERROR: cargo publish failed
    exit /b 1
)

echo [!P!] Publish OK
exit /b 0



:: =====================================================
:: Function: Help
:: =====================================================
:print_help
echo Usage: package_publish_workspace.bat [OPTIONS]
echo   --dry-run
echo   --skip-package
echo   --allow-dirty
echo   --project NAME
echo   --verbose
echo   --all-features
echo   --no-default-features
echo   --features "a,b,c"
echo   --help
goto :EOF
