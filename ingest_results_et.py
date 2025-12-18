import os
import sys
import time
from datetime import datetime, date
from zoneinfo import ZoneInfo

import psycopg2
import pandas as pd

from nba_api.stats.endpoints import leaguegamelog

ET = ZoneInfo("America/New_York")


def get_db_conn():
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    dbname = os.getenv("PGDATABASE", "nba")
    user = os.getenv("PGUSER", "nba_user")
    password = os.getenv("PGPASSWORD", "nba_pass")
    if not password:
        raise RuntimeError("PGPASSWORD not set. In PowerShell: $env:PGPASSWORD='your_password'")
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def season_str_from_et_date(d: date) -> str:
    start = d.year if d.month >= 10 else d.year - 1
    end = (start + 1) % 100
    return f"{start}-{end:02d}"


def upsert_game_final(conn, game: dict):
    sql = """
    INSERT INTO games (
        game_id,
        game_date,
        game_date_et,
        home_team_id,
        away_team_id,
        status,
        home_pts,
        away_pts,
        season_type
    )
    VALUES (
        %(game_id)s,
        %(game_date_et)s,
        %(game_date_et)s,
        %(home_team_id)s,
        %(away_team_id)s,
        %(status)s,
        %(home_pts)s,
        %(away_pts)s,
        %(season_type)s
    )
    ON CONFLICT (game_id) DO UPDATE
    SET
        game_date = EXCLUDED.game_date,
        game_date_et = EXCLUDED.game_date_et,
        home_team_id = EXCLUDED.home_team_id,
        away_team_id = EXCLUDED.away_team_id,
        status = EXCLUDED.status,
        home_pts = EXCLUDED.home_pts,
        away_pts = EXCLUDED.away_pts,
        season_type = EXCLUDED.season_type;
    """
    with conn.cursor() as cur:
        cur.execute(sql, game)


def fetch_leaguegamelog_df(start_et: date, end_et: date) -> pd.DataFrame:
    season = season_str_from_et_date(end_et)
    date_from = start_et.strftime("%m/%d/%Y")
    date_to = end_et.strftime("%m/%d/%Y")

    last_err = None
    for attempt in range(3):
        try:
            lg = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star="Regular Season",
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                league_id="00",
                timeout=60,
            )
            return lg.get_data_frames()[0]
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"LeagueGameLog failed after retries: {last_err}")


def build_final_games_from_lg(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []

    # Only completed rows (WL present) with points
    df = df[df["WL"].notna() & df["PTS"].notna()].copy()
    if df.empty:
        return []

    df["GAME_DATE_PARSED"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date
    df = df[df["GAME_DATE_PARSED"].notna()].copy()

    finals = []

    for game_id, g in df.groupby("GAME_ID"):
        if len(g) < 2:
            continue

        home_row = None
        away_row = None

        for _, r in g.iterrows():
            matchup = str(r["MATCHUP"])
            if "vs." in matchup:
                home_row = r
            elif "@" in matchup:
                away_row = r

        if home_row is None or away_row is None:
            continue

        finals.append({
            "game_id": str(game_id),
            "game_date_et": home_row["GAME_DATE_PARSED"],
            "home_team_id": int(home_row["TEAM_ID"]),
            "away_team_id": int(away_row["TEAM_ID"]),
            "home_pts": int(home_row["PTS"]),
            "away_pts": int(away_row["PTS"]),
            "status": "final",
            "season_type": "regular",
        })

    return finals


def ingest_results_window_et(start_et: date, end_et: date):
    df = fetch_leaguegamelog_df(start_et, end_et)
    finals = build_final_games_from_lg(df)

    conn = get_db_conn()
    conn.autocommit = False
    try:
        for g in finals:
            upsert_game_final(conn, g)
        conn.commit()

        counts = {}
        for g in finals:
            counts[g["game_date_et"]] = counts.get(g["game_date_et"], 0) + 1

        print(f"Ingested ET window: {start_et} -> {end_et}")
        if counts:
            for d in sorted(counts.keys()):
                print(f"ET {d}: upserted {counts[d]} FINAL games")
        print(f"Done. Total FINAL games upserted: {len(finals)}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    if len(sys.argv) == 3:
        start_et = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        end_et = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    elif len(sys.argv) == 2:
        start_et = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        end_et = start_et
    else:
        # default last 5 ET days (more robust than 3)
        today_et = datetime.now(ET).date()
        start_et = today_et
        end_et = today_et

    print(f"Ingesting FINAL results for ET window: {start_et} -> {end_et}")
    ingest_results_window_et(start_et, end_et)


if __name__ == "__main__":
    main()

# Add this to your ingest/evaluation pipeline
import pandas as pd
from sqlalchemy import text

def evaluate_predictions(engine):
    """
    Looks for predictions in prediction_history that have null actuals,
    joins with the games table to find finished games, and calculates error.
    """
    with engine.connect() as conn:
        # 1. Find predictions waiting for a result
        query = text("""
            UPDATE prediction_history ph
            SET 
                actual_home_score = g.home_pts,
                actual_away_score = g.away_pts,
                correct_winner = (
                    CASE 
                        WHEN (ph.pred_home_prob > 0.5 AND g.home_pts > g.away_pts) THEN TRUE
                        WHEN (ph.pred_home_prob < 0.5 AND g.home_pts < g.away_pts) THEN TRUE
                        ELSE FALSE 
                    END
                ),
                error_margin = (ph.pred_home_score - ph.pred_away_score) - (g.home_pts - g.away_pts),
                updated_at = NOW()
            FROM games g
            WHERE ph.game_id = g.game_id
              AND g.status = 'Final'
              AND ph.actual_home_score IS NULL;
        """)
        
        result = conn.execute(query)
        conn.commit()
        print(f"Graded {result.rowcount} pending predictions.")