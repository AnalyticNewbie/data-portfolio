@echo off
title NBA Prediction Pipeline v2.1
color 0A

:: --- CONFIGURATION ---
set PGHOST=localhost
set PGDATABASE=nba
set PGUSER=nba_user
set PGPASSWORD=nba_pass

:: 1. Activate the Virtual Environment
echo ========================================================
echo  ACTIVATING ENVIRONMENT...
echo ========================================================
call .venv\Scripts\activate

:: 2. Ingest Results from Yesterday
echo.
echo ========================================================
echo  STEP 1: INGESTING SCORES AND GRADING...
echo ========================================================
python ingest_results_et.py

:: 3. Ingest Upcoming Schedule
echo.
echo ========================================================
echo  STEP 2: UPDATING SCHEDULE...
echo ========================================================
python ingest_schedule.py

:: 4. Generate Predictions for Today
echo.
echo ========================================================
echo  STEP 3: GENERATING PREDICTIONS v2.1...
echo ========================================================
:: Get today's date in YYYY-MM-DD format
for /f %%i in ('powershell -command "Get-Date -Format 'yyyy-MM-dd'"') do set TODAY=%%i

echo Predicting for Date (AEDT): %TODAY%
python predict_scores_for_date.py %TODAY%

echo.
echo ========================================================
echo  PIPELINE COMPLETE.
echo ========================================================
pause