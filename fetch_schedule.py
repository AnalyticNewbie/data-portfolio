import os
import time
import sys
from datetime import datetime, timedelta
import psycopg2
from dotenv import load_dotenv
from nba_api.stats.endpoints import scoreboardv2

# Load Environment Variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

# Config
DAYS_AHEAD = 7

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def fetch_schedule():
    print(f"--- Fetching Schedule for next {DAYS_AHEAD} days ---")
    
    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=DAYS_AHEAD)

    print(f"Window: {start_date} to {end_date}")

    conn = get_db_conn()
    cur = conn.cursor()
    
    total_upserted = 0

    # Loop through each day
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%m/%d/%Y") # API format
        db_date_str = current_date.strftime("%Y-%m-%d") # DB format
        
        print(f"Checking {db_date_str}...", end=" ")
        
        try:
            sb = scoreboardv2.ScoreboardV2(game_date=date_str)
            games = sb.game_header.get_data_frame()
            
            if games is not None and not games.empty:
                count = 0
                for _, row in games.iterrows():
                    game_id = row['GAME_ID']
                    home_id = row['HOME_TEAM_ID']
                    away_id = row['VISITOR_TEAM_ID']
                    
                    # FIX: Insert into BOTH game_date and game_date_et
                    sql = """
                        INSERT INTO games (game_id, game_date, game_date_et, home_team_id, away_team_id, status)
                        VALUES (%s, %s, %s, %s, %s, 'Scheduled')
                        ON CONFLICT (game_id) DO NOTHING;
                    """
                    try:
                        cur.execute(sql, (
                            game_id, 
                            db_date_str, # Fills game_date
                            db_date_str, # Fills game_date_et
                            home_id, 
                            away_id
                        ))
                        count += 1
                        conn.commit() # Commit each successful row
                    except Exception as inner_e:
                        conn.rollback() # Reset transaction if individual game fails
                        # print(f"Skip {game_id}: {inner_e}")
                
                print(f"Found {count} games.")
                total_upserted += count
            else:
                print("No games.")

            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error: {e}")

        current_date += timedelta(days=1)

    conn.close()
    print(f"\nSUCCESS: Schedule updated. Added/Verified {total_upserted} games.")

if __name__ == "__main__":
    fetch_schedule()