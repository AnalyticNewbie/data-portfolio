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

    # 1. Run the Data Engine & Sync (Prep the new data first)
    run_command("python daily_update.py", "Running NBA Data Engine")
    run_command("python sync_portfolio.py", "Syncing Cloud Data to Local Website")

    # 2. Stage and Commit local changes (Clears the 'unstaged changes' error)
    run_command("git add .", "Staging all changes")
    # Using a generic message; Git needs a commit to allow a rebase
    run_command('git commit -m "Daily update and path fixes"', "Committing changes locally")

    # 3. NOW Pull/Rebase (This will work because your workspace is now clean)
    run_command("git pull origin main --rebase", "Syncing with GitHub")

    # 4. Final Push
    run_command("git push origin main", "Pushing to GitHub")

    print("\n========================================")
    print("🎊 DEPLOYMENT COMPLETE: Site is updating! 🎊")
    print("========================================")

if __name__ == "__main__":
    main()