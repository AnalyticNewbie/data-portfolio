import subprocess
import sys
import os
import psycopg2
import json
from datetime import datetime
try:
    import pytz
except ImportError:
    print("Installing missing package: pytz...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pytz"])
    import pytz

def send_to_db(payload, category):
    """Pushes the model output to the PostgreSQL cloud database."""
    db_url = os.getenv('DB_URL')
    if not db_url:
        print(f"⚠️ Warning: DB_URL not found. Skipping database upload for {category}.")
        return

    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # We use json.dumps to convert the list/dict into a format Postgres understands
        cur.execute("""
            INSERT INTO daily_intelligence (prediction_date, category, payload)
            VALUES (CURRENT_DATE, %s, %s)
        """, (category, json.dumps(payload)))
        
        conn.commit()
        print(f"✅ Cloud Sync: {category} data successfully pushed to database.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Database Error: {e}")

def run_script(script_name, args=[]):
    print(f"\n>>> Running {script_name}...")
    try:
        # Run script using the current Python interpreter
        subprocess.check_call([sys.executable, script_name] + args)
    except subprocess.CalledProcessError:
        print(f"!!! Error running {script_name}. Pipeline stopped.")
        sys.exit(1)

def get_target_date():
    """
    Returns the current date in Australia/Sydney Time.
    The downstream 'predict' scripts expect an AU date and will 
    handle the conversion to US Game Time automatically.
    """
    # FIX: Use Sydney time so the default date matches your wall clock
    au_tz = pytz.timezone('Australia/Sydney')
    now_au = datetime.now(au_tz)
    return now_au.strftime('%Y-%m-%d')

def main():
    # 1. Auto-Detect Date (Sydney Time)
    target_date = get_target_date()
    
    # Allow override via command line (e.g. python daily_update.py 2025-12-25)
    if len(sys.argv) > 1:
        target_date = sys.argv[1]

    print("========================================")
    print(f"   NBA PREDICTOR - DAILY PIPELINE")
    print(f"   Target Date (AU): {target_date}")
    print("========================================")

    # 2. Update Data
    run_script("fetch_schedule.py")     # Get future games
    run_script("fetch_latest_games.py") # Get yesterday's scores
    run_script("fetch_player_logs.py")  # Get player stats

    # 3. Retrain Models (Refreshes the 'brain' with yesterday's data)
    if os.path.exists("train_advanced_win.py"):
        run_script("train_advanced_win.py")
    if os.path.exists("train_advanced_margin.py"):
        run_script("train_advanced_margin.py")
    if os.path.exists("train_advanced_total.py"):
        run_script("train_advanced_total.py")

    # 4. Generate Predictions (Teams)
    print(f"\n>>> Generating Predictions for {target_date}...")
    run_script("predict_scores_for_date.py", [target_date])

    # 5. Generate Predictions (Players)
    if os.path.exists("predict_daily_props.py"):
        run_script("predict_daily_props.py", [target_date])

    # --- NEW LOGIC TO FETCH THE DATA ---
    print("\n>>> Collecting results for Cloud Sync...")
    
    # Load the team data that predict_scores_for_date.py just created
    team_results = []
    if os.path.exists("data.json"):
        with open("data.json", "r") as f:
            full_data = json.load(f)
            # Adjust these keys based on your actual data.json structure
            team_results = full_data.get("matchups", [])
            prop_results = full_data.get("props", [])

    # Now the variables exist, so we can send them!
    if team_results:
        send_to_db(team_results, 'Team')
    
    if prop_results:
        send_to_db(prop_results, 'Player')

    print("\n========================================")
    print("   PIPELINE COMPLETE")
    print("========================================")

if __name__ == "__main__":
    main()