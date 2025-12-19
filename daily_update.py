import sys
import os
import json
import subprocess
from datetime import datetime

def run_script(script_name, args=None):
    """Utility to run sub-scripts and handle errors."""
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    
    print(f">>> Executing: {script_name} {' '.join(args) if args else ''}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ Error in {script_name}")
        return False
    return True

def get_target_date():
    """Default to today's date if no override provided."""
    return datetime.now().strftime("%Y-%m-%d")

def main():
    # 1. Auto-Detect Date
    target_date = get_target_date()
    
    if len(sys.argv) > 1:
        target_date = sys.argv[1]

    print("========================================")
    print(f"   NBA PREDICTOR - DAILY PIPELINE")
    print(f"   Target Date (AU): {target_date}")
    print("========================================")

    # 2. Update Data
    run_script("fetch_schedule.py")     
    run_script("fetch_latest_games.py") 
    run_script("fetch_player_logs.py")  

    # 3. Retrain Models
    for model_script in ["train_advanced_win.py", "train_advanced_margin.py", "train_advanced_total.py"]:
        if os.path.exists(model_script):
            run_script(model_script)

    # 4. Generate Predictions (Teams)
    run_script("predict_scores_for_date.py", [target_date])

    # 5. Generate Predictions (Players)
    if os.path.exists("predict_daily_props.py"):
        run_script("predict_daily_props.py", [target_date])

    # 6. Final Sync & Path Correction
    print("\n>>> Finalizing Data for Portfolio Hub...")
    
    # Path where the Hub expects to find data
    json_path = "projects/nba-predictor/data.json"
    
    if os.path.exists(json_path):
        print(f"✅ Success: Data localized in {json_path}")
    else:
        print(f"⚠️ Warning: {json_path} not found. Ensure sub-scripts use this path.")

    # 7. Cloud Sync Trigger
   # un_script("sync_portfolio.py")

    print("\n========================================")
    print("   PIPELINE COMPLETE")
    print("========================================")

if __name__ == "__main__":
    main()