import sys
import os
import pandas as pd
import joblib
import psycopg2
import warnings
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()

# Fixed: Target the specific warning message using Regex
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy connectable.*")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

MODEL_PTS_PATH = "models/points_model.joblib"
MODEL_REB_PATH = "models/rebounds_model.joblib"
MODEL_AST_PATH = "models/assists_model.joblib"

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_games_for_date(target_date_et):
    conn = get_db_conn()
    cur = conn.cursor()
    # Join games to teams to get abbreviations
    sql = """
        SELECT g.game_id, g.home_team_id, g.away_team_id, 
               ht.team_abbr as home_abbr, at.team_abbr as away_abbr,
               g.game_date_et
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        WHERE g.game_date_et = %s
    """
    cur.execute(sql, (target_date_et,))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_team_roster(team_id):
    conn = get_db_conn()
    cur = conn.cursor()
    sql = "SELECT id, name FROM players WHERE team_id = %s AND current_status = 'Active'"
    cur.execute(sql, (team_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_player_features(player_id):
    conn = get_db_conn()
    # Fetch recent logs
    sql = """
        SELECT pts, reb, ast, min 
        FROM player_logs 
        WHERE player_id = %s 
        ORDER BY game_date DESC 
        LIMIT 20
    """
    df = pd.read_sql(sql, conn, params=(player_id,))
    conn.close()
    
    if len(df) < 5: return None
        
    # Calculate Features (must match training columns: lowercase)
    feats = {}
    l5 = df.head(5)
    l10 = df.head(10)
    
    feats['pts_l5'] = l5['pts'].mean()
    feats['reb_l5'] = l5['reb'].mean()
    feats['ast_l5'] = l5['ast'].mean()
    feats['min_l5'] = l5['min'].mean()
    
    feats['pts_l10'] = l10['pts'].mean()
    feats['reb_l10'] = l10['reb'].mean()
    feats['ast_l10'] = l10['ast'].mean()
    
    feats['pts_l20'] = df.head(20)['pts'].mean()
    
    return pd.DataFrame([feats])

def main():
    # 1. Handle AU Date Input
    if len(sys.argv) > 1:
        input_date = sys.argv[1]
    else:
        # Default to "Tomorrow" in US time (which is "Today" in AU)
        input_date = datetime.now().strftime('%Y-%m-%d')

    # Convert AU Date to US Game Date (Subtract 1 day)
    try:
        date_obj = datetime.strptime(input_date, "%Y-%m-%d")
        target_date_et = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        print("Error: Date must be YYYY-MM-DD")
        return

    print(f"\n=== PLAYER PROP CHEAT SHEET ===")
    print(f"AU Date: {input_date}")
    print(f"US Date: {target_date_et} (Used for Database Lookup)")
    
    # 2. Load Models
    try:
        pts_model = joblib.load(MODEL_PTS_PATH)
        reb_model = joblib.load(MODEL_REB_PATH)
        ast_model = joblib.load(MODEL_AST_PATH)
    except:
        print("Error: Models not found.")
        return

    # 3. Get Games
    games = get_games_for_date(target_date_et)
    if not games:
        print(f"No games found for {target_date_et} ET.")
        print("Tip: Run 'fetch_schedule.py' if you haven't recently.")
        return

    # 4. Generate Predictions
    for g in games:
        game_id, hid, aid, habbr, aabbr, _ = g
        print(f"\n>>> {aabbr} @ {habbr} <<<")
        print(f"{'PLAYER':<20} | {'PTS':<5} {'(L5)':<6} | {'REB':<5} | {'AST':<5}")
        print("-" * 60)
        
        rosters = [(aid, aabbr), (hid, habbr)]
        
        for team_id, team_code in rosters:
            players = get_team_roster(team_id)
            
            # If roster is empty, it means team_id is missing in DB
            if not players:
                print(f"   [!] No active players found for {team_code}. Run fetch_players.py!")
                continue

            team_preds = []
            
            for pid, name in players:
                X = get_player_features(pid)
                if X is None: continue
                    
                p_pts = pts_model.predict(X)[0]
                p_reb = reb_model.predict(X)[0]
                p_ast = ast_model.predict(X)[0]
                
                # Filter: Show players projected for > 15 pts OR > 8 assists
                if p_pts > 15.0 or p_ast > 8.0: 
                    team_preds.append({
                        'name': name, 'pts': p_pts, 'l5': X['pts_l5'].iloc[0],
                        'reb': p_reb, 'ast': p_ast
                    })
            
            # Sort by PTS
            team_preds.sort(key=lambda x: x['pts'], reverse=True)
            
            for p in team_preds[:6]: # Top 6 per team
                diff = p['pts'] - p['l5']
                diff_str = f"({p['l5']:.1f})"
                print(f"{p['name'][:18]:<20} | {p['pts']:<5.1f} {diff_str:<6} | {p['reb']:<5.1f} | {p['ast']:<5.1f}")
            
            print("-" * 60)

if __name__ == "__main__":
    main()