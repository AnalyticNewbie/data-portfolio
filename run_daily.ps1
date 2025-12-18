# NBA Predictor - Daily Execution Script
# Usage: .\run_daily.ps1

$Date = Get-Date -Format "yyyy-MM-dd"
Write-Host "--- STARTING NBA PIPELINE FOR $Date ---" -ForegroundColor Cyan

# 1. Update Data (Scores & Boxscores)
Write-Host "`n[1/3] Ingesting latest scores and stats..." -ForegroundColor Yellow
python init_season.py
python ingest_boxscores.py

# 2. Retrain Brain (Learn from yesterday's results)
Write-Host "`n[2/3] Retraining models with new trends..." -ForegroundColor Yellow
python train_baseline_win.py
python train_score_models.py

# 3. Predict Future (Get tonight's games)
# Note: We pass the date dynamically. 
# If running for "Tomorrow" (Australian Time), add (Get-Date).AddDays(1)
$TargetDate = (Get-Date).AddDays(1).ToString("yyyy-MM-dd")
Write-Host "`n[3/3] Generating predictions for $TargetDate (AEDT)..." -ForegroundColor Yellow
python predict_scores_for_date.py $TargetDate

Write-Host "`n--- PIPELINE COMPLETE ---" -ForegroundColor Green
Read-Host -Prompt "Press Enter to exit"