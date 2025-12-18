import os
import sys
import joblib
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_team_id(conn, abbreviation):
    """Convert 'LAL' to ID using 'team_abbr' column"""
    cur = conn.cursor()
    # FIXED: Changed 'abbreviation' to 'team_abbr'
    cur.execute("SELECT team_id FROM teams WHERE team_abbr = %s", (abbreviation,))
    res = cur.fetchone()
    return res[0] if res else None

def get_team_stats(conn, team_id, game_date):
    """
    Calculate the features for a team entering a specific date.
    """
    sql = """
        SELECT 
            game_date_et,
            CASE WHEN home_team_id = %s THEN home_pts - away_pts ELSE away_pts - home_pts END as point_diff
        FROM games 
        WHERE (home_team_id = %s OR away_team_id = %s)
          AND status = 'Final'
          AND game_date_et < %s
        ORDER BY game_date_et DESC
        LIMIT 10
    """
    df = pd.read_sql(sql, conn, params=(team_id, team_id, team_id, game_date))
    
    if len(df) < 10:
        print(f"Warning: Team {team_id} has fewer than 10 games history. Prediction may be unstable.")
        if len(df) == 0:
            return None # Handle no history gracefully
    
    # 1. Rest Days
    last_game_date = df.iloc[0]['game_date_et']
    rest_days = (pd.to_datetime(game_date).date() - last_game_date).days
    
    # 2. Rolling Averages
    rolling_5 = df.head(5)['point_diff'].mean()
    rolling_10 = df.head(10)['point_diff'].mean()
    
    # 3. Strength of Schedule
    sos_sql = """
        SELECT rolling_sos_10 
        FROM v_strength_of_schedule 
        WHERE team_id = %s 
        ORDER BY game_id DESC LIMIT 1
    """
    cur = conn.cursor()
    cur.execute(sos_sql, (team_id,))
    sos_res = cur.fetchone()
    rolling_sos = sos_res[0] if sos_res else 0
    
    return {
        "rolling_pd_5": rolling_5,
        "rolling_pd_10": rolling_10,
        "rest_days": rest_days,
        "rolling_sos_10": rolling_sos
    }

def predict_matchup(home_abbr, away_abbr):
    conn = get_db_conn()
    model = joblib.load("models/win_model.joblib")
    
    try:
        home_id = get_team_id(conn, home_abbr)
        away_id = get_team_id(conn, away_abbr)
        
        if not home_id or not away_id:
            print(f"Error: Could not find team IDs for {home_abbr} or {away_abbr}. Check 'team_abbr' in database.")
            return

        today = datetime.now().strftime("%Y-%m-%d")
        
        # Get Stats
        h_stats = get_team_stats(conn, home_id, today)
        a_stats = get_team_stats(conn, away_id, today)
        
        if not h_stats or not a_stats:
            print("Error: Not enough history to predict this matchup.")
            return

        # Construct Feature Vector
        features = {
            "rolling_pd_5_diff": h_stats['rolling_pd_5'] - a_stats['rolling_pd_5'],
            "rolling_pd_10_diff": h_stats['rolling_pd_10'] - a_stats['rolling_pd_10'],
            "rest_days_diff": h_stats['rest_days'] - a_stats['rest_days'],
            "home_back_to_back": 1 if h_stats['rest_days'] == 1 else 0, 
            "home_advantage": 1,
            "sos_diff": h_stats['rolling_sos_10'] - a_stats['rolling_sos_10'],
            "day_of_week": 0 # Placeholder if needed, or mapping logic
        }
        
        # Fix Day of Week Mapping
        py_dow = datetime.now().weekday() # 0=Mon
        pg_dow_map = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 0} # Mon(0)->1 ... Sun(6)->0
        features['day_of_week'] = pg_dow_map[py_dow]

        # Convert to DataFrame
        X = pd.DataFrame([features])
        
        # Predict
        prob = model.predict_proba(X)[0][1]
        winner = home_abbr if prob > 0.5 else away_abbr
        
        print(f"\n--- Prediction: {home_abbr} (Home) vs {away_abbr} (Away) ---")
        print(f"Home Win Probability: {prob:.1%}")
        print(f"Predicted Winner:     {winner}")
        print("-" * 30)
        print(f"Key Factors:")
        print(f"  Home Rest: {h_stats['rest_days']} days | Away Rest: {a_stats['rest_days']} days")
        print(f"  Home Recent Form (L5): {h_stats['rolling_pd_5']:.1f} pts")
        print(f"  Away Recent Form (L5): {a_stats['rolling_pd_5']:.1f} pts")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) == 3:
        predict_matchup(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python predict_game.py [HOME_ABBR] [AWAY_ABBR]")
        print("Example: python predict_game.py GSW LAL")