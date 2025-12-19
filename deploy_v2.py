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

   # 1. Run the Data Pipeline (Generates new data)
    run_command("python daily_update.py", "Running NBA Data Engine")
    run_command("python sync_portfolio.py", "Syncing Database to Local JSON")

    # 2. Stage and Commit LOCAL changes first
    run_command("git add .", "Staging all changes")
    commit_msg = "Automated daily update"
    run_command(f'git commit -m "{commit_msg}"', "Committing local changes")

    # 3. NOW Pull/Rebase (Your changes are safe in a commit, so rebase will work)
    run_command("git pull origin main --rebase", "Syncing with GitHub")

    # 4. Push everything
    run_command("git push origin main", "Pushing to GitHub")

    print("\n========================================")
    print("🎊 DEPLOYMENT COMPLETE: Site is updating! 🎊")
    print("========================================")

if __name__ == "__main__":
    main()