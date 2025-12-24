import sys, os, subprocess, shutil, psycopg2
from datetime import datetime
import pytz

def run_script(script_name, args=None):
    cmd = [sys.executable, script_name]
    if args: cmd.extend(args)
    print(f"\n>>> Executing: {script_name}")
    result = subprocess.run(cmd)
    return result.returncode == 0

def get_nba_target_date():
    tz_et = pytz.timezone('US/Eastern')
    return datetime.now(tz_et).strftime("%Y-%m-%d")

def main():
    target_date = get_nba_target_date()
    if len(sys.argv) > 1: target_date = sys.argv[1]

    print(f"--- DAILY PIPELINE: {target_date} ---")

    # 1. Update Core Data
    run_script("fetch_schedule.py")     
    run_script("fetch_latest_games.py") 
    run_script("fetch_player_logs.py")  

    # 2. Update Officiating Intelligence
   # print("\n>>> Patching Referee Metadata...")
   # run_script("backfill_officials.py") 
   # run_script("patch_player_fouls.py")
   # run_script("patch_games_period_fouls.py") 

    # 3. Refresh the Master Ref Profiles View
  #  try:
   #     from db_config import get_db_connection
   #     conn = get_db_connection()
   #     with conn.cursor() as cur:
   #         print(">>> Refreshing Ref Master Profiles View...")
   ##         cur.execute("REFRESH MATERIALIZED VIEW mv_ref_master_profiles;")
   #         conn.commit()
   #     conn.close()
   # except Exception as e:
   #     print(f"⚠️ Warning: Could not refresh ref view: {e}")

    # 4. Retrain Models
    models = ["train_advanced_win.py", "train_advanced_margin.py", "train_advanced_total.py"]
    for model_script in models:
        if os.path.exists(model_script): run_script(model_script)

    # 5. Generate Predictions
    run_script("predict_scores_for_date.py", [target_date])
    if os.path.exists("predict_daily_props.py"):
        run_script("predict_daily_props.py", [target_date])

    # 6. Final Sync
    if os.path.exists("data.json"):
        shutil.copy2("data.json", "projects/nba-predictor/data.json")
        print("✅ Success: Master data synced.")

if __name__ == "__main__":
    main()