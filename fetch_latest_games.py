import os
import sys
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from nba_api.stats.endpoints import leaguegamefinder

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def update_games():
    print("--- Updating Game Scores ---")
    
    # 1. Fetch all games for this season (2025-26)
    try:
        gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2025-26', league_id_nullable='00')
        df = gamefinder.get_data_frames()[0]
    except Exception as e:
        print(f"Error fetching from NBA API: {e}")
        return

    conn = get_db_conn()
    cur = conn.cursor()
    
    games_by_id = df.groupby('GAME_ID')
    
    new_count = 0
    error_count = 0
    
    print(f"Processing {len(games_by_id)} games from API...")

    for game_id, rows in games_by_id:
        if len(rows) != 2:
            continue 
            
        row1 = rows.iloc[0]
        row2 = rows.iloc[1]
        
        # Identify Home vs Away
        if '@' in row1['MATCHUP']:
            away_row, home_row = row1, row2
        else:
            home_row, away_row = row1, row2
            
        try:
            # FIX: We insert into BOTH game_date and game_date_et
            sql = """
                INSERT INTO games (
                    game_id, game_date, game_date_et, home_team_id, away_team_id, 
                    home_pts, away_pts, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    home_pts = EXCLUDED.home_pts,
                    away_pts = EXCLUDED.away_pts,
                    status = EXCLUDED.status,
                    game_date = EXCLUDED.game_date; 
            """
            
            # The API gives date as YYYY-MM-DD
            g_date = home_row['GAME_DATE']

            cur.execute(sql, (
                game_id,
                g_date, # Fills 'game_date' (Required)
                g_date, # Fills 'game_date_et' (For consistency)
                int(home_row['TEAM_ID']),
                int(away_row['TEAM_ID']),
                int(home_row['PTS']),
                int(away_row['PTS']),
                'Final'
            ))
            
            conn.commit()
            new_count += 1
            
        except Exception as e:
            conn.rollback() # Important: Reset if one fails so others can continue
            print(f"Error inserting game {game_id}: {e}")
            error_count += 1

    conn.close()
    print(f"Processed. Success: {new_count}, Errors: {error_count}")

if __name__ == "__main__":
    update_games()