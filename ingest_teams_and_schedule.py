import os
from datetime import date, timedelta
import time
import pandas as pd
from dotenv import load_dotenv
import psycopg

from nba_api.stats.static import teams as nba_teams
from nba_api.stats.endpoints import leaguegamefinder

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "nba_pass")

def get_conn():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        autocommit=True,
    )

def upsert_teams(conn):
    team_list = nba_teams.get_teams()
    with conn.cursor() as cur:
        for t in team_list:
            cur.execute(
                """
                INSERT INTO teams (team_id, team_abbr, team_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (team_id) DO UPDATE
                SET team_abbr = EXCLUDED.team_abbr,
                    team_name = EXCLUDED.team_name;
                """,
                (t["id"], t["abbreviation"], t["full_name"]),
            )
    print(f"Upserted {len(team_list)} teams.")

def season_from_date(d: date) -> int:
    # NBA season year label (e.g., 2024-25 => 2024)
    # Season starts around Oct; simple rule: if month >= 10, season = year, else season = year-1
    return d.year if d.month >= 10 else d.year - 1

def fetch_games_for_date(d: date) -> pd.DataFrame:
    # Using LeagueGameFinder filtered by date; returns both team rows for each game.
    # We’ll collapse to one row per game.
    d_str = d.strftime("%m/%d/%Y")
    gf = leaguegamefinder.LeagueGameFinder(date_from_nullable=d_str, date_to_nullable=d_str)
    # Be polite to the stats API
    time.sleep(0.6)
    df = gf.get_data_frames()[0]
    return df

def upsert_games_for_date(conn, d: date):
    df = fetch_games_for_date(d)
    if df.empty:
        print(f"No games found for {d}.")
        return

    # Each game appears twice (one per team). We need home/away and final points.
    # MATCHUP column looks like: "LAL vs. BOS" (home) or "LAL @ BOS" (away).
    games = {}

    for _, r in df.iterrows():
        game_id = r["GAME_ID"]
        team_id = int(r["TEAM_ID"])
        pts = int(r["PTS"]) if pd.notna(r["PTS"]) else None
        matchup = r["MATCHUP"]

        if game_id not in games:
            games[game_id] = {"game_id": game_id, "date": d, "home_team_id": None, "away_team_id": None, "home_pts": None, "away_pts": None}

        if "vs." in matchup:
            games[game_id]["home_team_id"] = team_id
            games[game_id]["home_pts"] = pts
        elif "@" in matchup:
            games[game_id]["away_team_id"] = team_id
            games[game_id]["away_pts"] = pts

    # Determine status: if points exist, it’s final (for that date’s completed games)
    with conn.cursor() as cur:
        for g in games.values():
            if g["home_team_id"] is None or g["away_team_id"] is None:
                # Skip malformed entries
                continue

            status = "final" if (g["home_pts"] is not None and g["away_pts"] is not None) else "scheduled"
            season = season_from_date(d)

            cur.execute(
                """
                INSERT INTO games (game_id, game_date, season, home_team_id, away_team_id, status, home_pts, away_pts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE
                SET game_date = EXCLUDED.game_date,
                    season = EXCLUDED.season,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    status = EXCLUDED.status,
                    home_pts = EXCLUDED.home_pts,
                    away_pts = EXCLUDED.away_pts,
                    updated_at = now();
                """,
                (g["game_id"], g["date"], season, g["home_team_id"], g["away_team_id"], status, g["home_pts"], g["away_pts"]),
            )

    print(f"Upserted {len(games)} games for {d}.")

def main():
    # For v1, ingest a rolling window: yesterday, today, tomorrow
    today = date.today()
    dates = [today - timedelta(days=1), today - timedelta(days=2)]

    with get_conn() as conn:
        upsert_teams(conn)
        for d in dates:
            upsert_games_for_date(conn, d)

if __name__ == "__main__":
    main()