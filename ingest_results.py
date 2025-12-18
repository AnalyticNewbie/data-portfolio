import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import psycopg

from nba_api.stats.endpoints import boxscoretraditionalv2

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "nba_pass")

ET = ZoneInfo("America/New_York")

API_SLEEP_SECONDS = 0.7  # be polite to NBA API

def get_conn():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        autocommit=True,
    )

def get_games_pending_results():
    today_et = datetime.now(ET).date()

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              g.game_id,
              g.home_team_id,
              g.away_team_id
            FROM games g
            WHERE g.status = 'scheduled'
              AND g.game_date_et < %s
            ORDER BY g.game_date_et;
            """,
            (today_et,)
        )
        return cur.fetchall()

def ingest_results():
    pending_games = get_games_pending_results()

    if not pending_games:
        print("No pending results to ingest.")
        return

    print(f"Ingesting results for {len(pending_games)} games...")

    with get_conn() as conn, conn.cursor() as cur:
        for game_id, home_team_id, away_team_id in pending_games:
            try:
                bs = boxscoretraditionalv2.BoxScoreTraditionalV2(
                    game_id=game_id
                )

                time.sleep(API_SLEEP_SECONDS)

                team_stats = bs.team_stats.get_data_frame()
                if team_stats is None or team_stats.empty:
                    print(f"{game_id}: no team stats yet")
                    continue

                # Identify rows
                home_row = team_stats[team_stats["TEAM_ID"] == home_team_id]
                away_row = team_stats[team_stats["TEAM_ID"] == away_team_id]

                if home_row.empty or away_row.empty:
                    print(f"{game_id}: incomplete stats")
                    continue

                home_pts = int(home_row.iloc[0]["PTS"])
                away_pts = int(away_row.iloc[0]["PTS"])

                cur.execute(
                    """
                    UPDATE games
                    SET home_pts = %s,
                        away_pts = %s,
                        status = 'final',
                        updated_at = now()
                    WHERE game_id = %s;
                    """,
                    (home_pts, away_pts, game_id)
                )

                print(
                    f"{game_id}: FINAL {home_pts}-{away_pts}"
                )

            except Exception as e:
                print(f"{game_id}: error ingesting result -> {e}")

    print("Results ingestion complete.")

if __name__ == "__main__":
    ingest_results()