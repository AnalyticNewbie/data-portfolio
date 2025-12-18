import sys
import os
import pandas as pd
import joblib
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def find_player_id(name_fragment):
    conn = get_db_conn()
    cur = conn.cursor()
    # Case-insensitive search
    cur.execute("SELECT id, name FROM players WHERE name ILIKE %s LIMIT 1", (f"%{name_fragment}%",))
    res = cur.fetchone()
    conn.close()
    return res if res else (None, None)

def get_player_recent_stats(player_id):
    """Fetch last 20 games to calculate current rolling stats."""
    conn = get_db_conn()
    # Get last 20 games ordered by date
    sql = """
        SELECT game_date, pts, reb, ast, min 
        FROM player_logs 
        WHERE player_id = %s 
        ORDER BY game_date DESC 
        LIMIT 20
    """
    df = pd.read_sql(sql, conn, params=(player_id,))
    conn.close()
    return df

def predict_player(player_name_input):
    # 1. Find Player
    pid, full_name = find_player_id(player_name_input)
    if not pid:
        print(f"Error: Player '{player_name_input}' not found.")
        return

    # 2. Get Data
    df = get_player_recent_stats(pid)
    if len(df) < 5:
        print(f"Not enough data for {full_name} (Found {len(df)} games). Need at least 5.")
        return

    # 3. Calculate Features (The "L5", "L10" stats entering the NEXT game)
    # The model expects columns: pts_l5, reb_l5, ast_l5, min_l5, pts_l10...
    
    current_stats = {}
    
    # L5
    last_5 = df.head(5)
    current_stats['pts_l5'] = last_5['pts'].mean()
    current_stats['reb_l5'] = last_5['reb'].mean()
    current_stats['ast_l5'] = last_5['ast'].mean()
    current_stats['min_l5'] = last_5['min'].mean()
    
    # L10
    last_10 = df.head(10)
    current_stats['pts_l10'] = last_10['pts'].mean()
    current_stats['reb_l10'] = last_10['reb'].mean()
    current_stats['ast_l10'] = last_10['ast'].mean()
    
    # L20
    last_20 = df.head(20)
    current_stats['pts_l20'] = last_20['pts'].mean()

    # Create DataFrame for Model
    X = pd.DataFrame([current_stats])
    
    # 4. Load Models & Predict
    try:
        pts_model = joblib.load("models/points_model.joblib")
        reb_model = joblib.load("models/rebounds_model.joblib")
        ast_model = joblib.load("models/assists_model.joblib")
        
        pred_pts = pts_model.predict(X)[0]
        pred_reb = reb_model.predict(X)[0]
        pred_ast = ast_model.predict(X)[0]
        
        print(f"\n--- Projections for {full_name} ---")
        print(f"Last Game: {df.iloc[0]['game_date']} ({int(df.iloc[0]['pts'])} pts)")
        print("-" * 30)
        print(f"POINTS:   {pred_pts:.1f}  (L5 Avg: {current_stats['pts_l5']:.1f})")
        print(f"REBOUNDS: {pred_reb:.1f}  (L5 Avg: {current_stats['reb_l5']:.1f})")
        print(f"ASSISTS:  {pred_ast:.1f}  (L5 Avg: {current_stats['ast_l5']:.1f})")
        print("-" * 30)
        
    except FileNotFoundError:
        print("Error: Models not found. Run train_player_props.py first.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python predict_player_props.py [PLAYER NAME]")
    else:
        # Join all arguments to allow "LeBron James" without quotes
        name_query = " ".join(sys.argv[1:])
        predict_player(name_query)