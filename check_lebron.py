import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_lebron():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "nba"),
        user=os.getenv("DB_USER", "nba_user"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()
    
    print("--- Checking LeBron James Team Status ---")
    
    # Updated Query: Using 'team_abbr' which we know exists
    sql = """
        SELECT p.name, p.team_id, t.team_abbr
        FROM players p
        JOIN teams t ON p.team_id = t.team_id
        WHERE p.name ILIKE '%LeBron James%';
    """
    
    try:
        cur.execute(sql)
        row = cur.fetchone()
        
        if row:
            player_name, team_id, team_abbr = row
            print(f"Player:   {player_name}")
            print(f"Team ID:  {team_id}")
            print(f"Team:     {team_abbr}")
            
            if team_abbr == 'CLE':
                print("\n[!] DIAGNOSIS: Stale Data. Database thinks LeBron is on Cleveland.")
                print("    ACTION: Run 'python fetch_players.py' to update rosters.")
            elif team_abbr == 'LAL':
                print("\n[OK] DIAGNOSIS: Database is correct (Lakers).")
            else:
                print(f"\n[?] DIAGNOSIS: LeBron is on {team_abbr}. This is unexpected.")
        else:
            print("Player 'LeBron James' not found in database.")
            
    except Exception as e:
        print(f"Error: {e}")

    conn.close()

if __name__ == "__main__":
    check_lebron()