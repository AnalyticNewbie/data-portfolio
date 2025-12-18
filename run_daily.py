import subprocess
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def run(cmd: list[str]):
    print("\n>>", " ".join(cmd))
    subprocess.check_call(cmd)


def main():
    print("Running daily NBA pipeline...")

    print("\n1) Ingesting schedule...")
    run(["python", "ingest_schedule.py"])

    today_et = datetime.now(ET).date()
    start_et = today_et - timedelta(days=5)
    end_et = today_et

    print("\n2) Ingesting results (ET window backfill)...")
    run(["python", "ingest_results_et.py", str(start_et), str(end_et)])

    print("\n3) Materialising rest days...")
    run(["powershell", "-Command",
         "Get-Content sql/features/rest_days_view.sql | docker exec -i nba_postgres psql -U nba_user -d nba; "
         "Get-Content sql/materialise/rest_days_upsert.sql | docker exec -i nba_postgres psql -U nba_user -d nba"
    ])

    print("\n4) Materialising rolling form...")
    run(["powershell", "-Command",
         "Get-Content sql/features/rolling_form_view.sql | docker exec -i nba_postgres psql -U nba_user -d nba; "
         "Get-Content sql/materialise/rolling_pd_upsert.sql | docker exec -i nba_postgres psql -U nba_user -d nba"
    ])

    print("\nDone.")


if __name__ == "__main__":
    main()
