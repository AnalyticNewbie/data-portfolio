import subprocess
import sys

def run_command(command, description):
    print(f"\n>>> {description}...")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"❌ Error during: {description}")
        sys.exit(1)
    print(f"✅ Finished: {description}")

def main():
    print("🚀 STARTING FULL V2.3 DEPLOYMENT 🚀")

    # 1. Run the Data Pipeline
    run_command("python daily_update.py", "Running NBA Data Engine")

    # 2. Run the Sync Script (to ensure data.json is perfectly aligned with DB)
    run_command("python sync_portfolio.py", "Syncing Database to Local JSON")

    # 3. Git Maintenance (The 'Clear Sidebar' Logic)
    run_command("git add .", "Staging all changes")
    
    # We use a generic message or you can customize it
    commit_msg = "Automated daily update and portfolio sync"
    run_command(f'git commit -m "{commit_msg}"', "Committing to local Git")

    # 4. Push to GitHub
    run_command("git push origin main", "Pushing to GitHub")

    print("\n========================================")
    print("🎊 DEPLOYMENT COMPLETE: Site is updating! 🎊")
    print("========================================")

if __name__ == "__main__":
    main()