import subprocess, shutil, os, sys, json, pytz
from datetime import datetime, timedelta

def run_command(command, description):
    print(f"\n>>> {description}...")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"❌ Error during: {description}")
        sys.exit(1)
    print(f"✅ Finished: {description}")

def get_nba_target_date():
    """Calculates the current NBA ET date (US Eastern)"""
    tz_et = pytz.timezone('US/Eastern')
    now_et = datetime.now(tz_et)
    # NBA days typically cycle at 4 AM ET
    if now_et.hour < 4:
        return (now_et - timedelta(days=1)).strftime("%Y-%m-%d")
    return now_et.strftime("%Y-%m-%d")

def create_master_json(target_date):
    print(f"\n🔍 MERGING DATA FOR NBA DATE: {target_date}")
    
    # 1. Start with the Games (from data.json)
    if os.path.exists("data.json"):
        with open("data.json", "r", encoding='utf-8') as f:
            try:
                master_data = json.load(f)
            except:
                print("❌ Error reading data.json")
                return
    else:
        print("❌ data.json not found.")
        return

    # 2. Merge Player Props (from data_fresh.json)
    if os.path.exists("data_fresh.json"):
        with open("data_fresh.json", "r", encoding='utf-8') as f:
            try:
                prop_data = json.load(f)
                master_data["top_props"] = prop_data.get("projections", [])
                print(f"✅ Merged {len(master_data['top_props'])} props.")
            except:
                print("❌ Error reading data_fresh.json")

    # 3. Save Final Master
    with open("data.json", "w", encoding='utf-8') as f:
        json.dump(master_data, f, indent=4, ensure_ascii=False)

def main():
    print("🚀 STARTING MASTER DEPLOYMENT v2.5.0 🚀")
    run_command("git pull --rebase --autostash origin main", "Syncing with GitHub")
    
    # Use the shared US Date for all scripts
    nba_date = get_nba_target_date()

    # Pass the date to the pipeline
    run_command(f"python daily_update.py {nba_date}", "Executing NBA Data Pipeline")

    # Perform the merge
    create_master_json(nba_date)
    
    hub_path = "projects/nba-predictor/data.json"
    if os.path.exists("data.json"):
        shutil.copy2("data.json", hub_path)
    
    # Use 'git add -u' to stage all modified and deleted files
    run_command("git add -u", "Staging All Modified Files")
    run_command(f"git add {hub_path}", "Staging Hub Data")
    
    status = subprocess.run("git status --porcelain", shell=True, capture_output=True, text=True)
    if status.stdout.strip():
        run_command(f'git commit -m "Automated NBA Sync: {nba_date}"', "Committing")
        run_command("git push origin main", "Pushing to GitHub")
    else:
        print("ℹ️ No new data for deployment.")

    print(f"\n🎊 MASTER DEPLOYMENT COMPLETE 🎊")

if __name__ == "__main__":
    main()