import os
import time
from datetime import datetime
from typing import List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from nba_api.stats.endpoints import leaguegamefinder


# -----------------------------
# Configuration
# -----------------------------
SEASONS = ["2022-23", "2023-24", "2024-25"]
SEASON_TYPE_API = "Regular Season"   # NBA API value
SEASON_TYPE_DB = "regular"           # what we store in DB

SLEEP_SECONDS = 0.6                  # NBA API rate limiting


# -----------------------------
# Database connection
# -----------------------------
def get_db_conn():
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    dbname = os.getenv("PGDATABASE", "nba")
    user = os.getenv("PGUSER", "nba_user")
    password = os.getenv("PGPASSWORD", "nba_pass")

    if not password:
        raise RuntimeError("PGPASSWORD environment variable is not set")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


# -----------------------------
# NBA API helpers
# -----------------------------
def fetch_team_game_rows(season: str) -> pd.DataFrame:
    """
    Fetch one row per TEAM per GAME from NBA API.
    """
    print(f"Fetching {season} ({SEASON_TYPE_API})...")
    lgf = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        season_type_nullable=SEASON_TYPE_API,
    )
    df = lgf.get_data_frames()[0]
    time.sleep(SLEEP_SECONDS)
    return df


def combine_home_away(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert team-level rows into one row per game:
      game_id, game_date, home_team_id, away_team_id, home_pts, away_pts
    """
    if df.empty:
        return df

    df = df.copy()
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"]).dt.date

    # Identify home vs away from MATCHUP string
    df["is_home"] = df["MATCHUP"].str.contains("vs.", regex=False)
    df["is_away"] = df["MATCHUP"].str.contains("@", regex=False)

    home = (
        df[df["is_home"]]
        .loc[:, ["GAME_ID", "GAME_DATE", "TEAM_ID", "PTS"]]
        .rename(
            columns={
                "GAME_ID": "game_id",
                "GAME_DATE": "game_date",
                "TEAM_ID": "home_team_id",
                "PTS": "home_pts",
            }
        )
    )

    away = (
        df[df["is_away"]]
        .loc[:, ["GAME_ID", "TEAM_ID", "PTS"]]
        .rename(
            columns={
                "GAME_ID": "game_id",
                "TEAM_ID": "away_team_id",
                "PTS": "away_pts",
            }
        )
    )

    games = home.merge(away, on="game_id", how="inner")
    games = games.drop_duplicates(subset=["game_id"])

    return games


# -----------------------------
# Database upsert
# -----------------------------
def upsert_games(conn, games_df: pd.DataFrame) -> int:
    if games_df.empty:
        return 0

    rows = []
    for _, r in games_df.iterrows():
        rows.append(
            (
                str(r["game_id"]),
                r["game_date"],                 # satisfies NOT NULL game_date
                r["game_date"],                 # game_date_et (same date)
                int(r["home_team_id"]),
                int(r["away_team_id"]),
                int(r["home_pts"]),
                int(r["away_pts"]),
                "final",
                SEASON_TYPE_DB,
            )
        )

    sql = """
        INSERT INTO games (
            game_id,
            game_date,
            game_date_et,
            home_team_id,
            away_team_id,
            home_pts,
            away_pts,
            status,
            season_type
        )
        VALUES %s
        ON CONFLICT (game_id) DO UPDATE
        SET
            game_date     = EXCLUDED.game_date,
            game_date_et  = EXCLUDED.game_date_et,
            home_team_id  = EXCLUDED.home_team_id,
            away_team_id  = EXCLUDED.away_team_id,
            home_pts      = EXCLUDED.home_pts,
            away_pts      = EXCLUDED.away_pts,
            status        = EXCLUDED.status,
            season_type   = EXCLUDED.season_type;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)

    conn.commit()
    return len(rows)


# -----------------------------
# Main
# -----------------------------
def main(seasons: Optional[List[str]] = None):
    seasons = seasons or SEASONS
    conn = get_db_conn()

    total = 0
    try:
        for season in seasons:
            team_rows = fetch_team_game_rows(season)
            games_df = combine_home_away(team_rows)
            count = upsert_games(conn, games_df)
            total += count
            print(f"{season}: upserted {count} regular-season final games")

        print(f"\nBackfill complete. Total games upserted: {total}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
