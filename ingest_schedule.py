import os
import time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import psycopg

from nba_api.stats.endpoints import scoreboardv2

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "nba_pass")

# Schedule window
DAYS_BACK = 3
DAYS_AHEAD = 7

ET = ZoneInfo("America/New_York")
AEDT = ZoneInfo("Australia/Sydney")

def get_conn():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        autocommit=True,
    )

def season_from_date(d: date) -> int:
    # Season label by starting year, e.g., 2025-26 -> 2025
    return d.year if d.month >= 10 else d.year - 1

def to_int_or_none(x):
    if x is None:
        return None
    try:
        return int(float(x))
    except Exception:
        return None

def ingest_schedule():
    today_et = datetime.now(ET).date()
    start_et = today_et - timedelta(days=DAYS_BACK)
    end_et = today_et + timedelta(days=DAYS_AHEAD)

    print(f"Ingesting schedule from {start_et} to {end_et} (ET source).")

    all_games = []
    days_total = (end_et - start_et).days + 1

    for i in range(days_total):
        game_date_et = start_et + timedelta(days=i)
        date_str = game_date_et.strftime("%m/%d/%Y")

        sb = scoreboardv2.ScoreboardV2(game_date=date_str)

        # Be polite to the stats API
        time.sleep(0.6)

        gh = sb.game_header.get_data_frame()
        if gh is None or gh.empty:
            continue

        for _, row in gh.iterrows():
            game_id = row.get("GAME_ID")
            home_id = to_int_or_none(row.get("HOME_TEAM_ID"))
            away_id = to_int_or_none(row.get("VISITOR_TEAM_ID"))

            # Skip non-standard rows / incomplete rows
            if not game_id or home_id is None or away_id is None:
                continue

            all_games.append({
                "game_id": str(game_id),
                "game_date_et": game_date_et,
                "home_team_id": home_id,
                "away_team_id": away_id,
            })

    if not all_games:
        print("No scheduled games found in the requested window.")
        return

    with get_conn() as conn, conn.cursor() as cur:
        for g in all_games:
            cur.execute(
                """
                INSERT INTO games (
                    game_id,
                    game_date,
                    season,
                    home_team_id,
                    away_team_id,
                    status,
                    home_pts,
                    away_pts,
                    game_date_et
                )
                VALUES (%s, %s, %s, %s, %s, 'scheduled', NULL, NULL, %s)
                ON CONFLICT (game_id) DO UPDATE
                SET game_date = EXCLUDED.game_date,
                    season = EXCLUDED.season,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    status = 'scheduled',
                    game_date_et = EXCLUDED.game_date_et,
                    updated_at = now();
                """,
                (
                    g["game_id"],
                    # Keep game_date aligned with ET for now (canonical “NBA day”)
                    g["game_date_et"],
                    season_from_date(g["game_date_et"]),
                    g["home_team_id"],
                    g["away_team_id"],
                    g["game_date_et"],
                )
            )

    print(f"Upserted {len(all_games)} scheduled games.")

if __name__ == "__main__":
    ingest_schedule()