import sys
import os
import joblib
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
DB_HOST = os.getenv("PGHOST", "localhost")
DB_NAME = os.getenv("PGDATABASE", "nba")
DB_USER = os.getenv("PGUSER", "nba_user")
DB_PASS = os.getenv("PGPASSWORD")

MODEL_WIN_PATH = "models/win_model.joblib"     
MODEL_MARGIN_PATH = "models/margin_model.joblib"
MODEL_TOTAL_PATH = "models/total_model.joblib"

# -------------------------------------------------------------------
# DB Connection
# -------------------------------------------------------------------
def get_db_conn():
    if not DB_PASS:
        print("Error: PGPASSWORD environment variable not set.")
        sys.exit(1)
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

# -------------------------------------------------------------------
# Data Fetching (Matches predict_scores_v2.1.py)
# -------------------------------------------------------------------
def fetch_games_for_backtest(conn, game_date_et):
    sql = """
        WITH game_features AS (
            SELECT 
                g.game_id,
                g.game_date_et,
                ht.team_abbr as home_team,
                at.team_abbr as away_team,
                g.home_pts as actual_home,  -- We fetch actuals for grading
                g.away_pts as actual_away,
                
                -- MODEL INPUTS
                (hf.rolling_pd_5 - af.rolling_pd_5) as rolling_pd_5_diff,
                (hf.rolling_pd_10 - af.rolling_pd_10) as rolling_pd_10_diff,
                (hf.rest_days - af.rest_days) as rest_days_diff,
                CASE WHEN hf.rest_days = 0 THEN 1 ELSE 0 END as home_back_to_back,
                1 as home_advantage,
                (COALESCE(hsos.rolling_sos_10, 0) - COALESCE(asos.rolling_sos_10, 0)) as sos_diff
                
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.team_id
            JOIN teams at ON g.away_team_id = at.team_id
            LEFT JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
            LEFT JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
            LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
            LEFT JOIN v_strength_of_schedule asos ON g.away_team_id = asos.team_id AND g.game_id = asos.game_id
            
            WHERE g.game_date_et = %s 
              AND g.home_pts IS NOT NULL -- Only finished games
        )
        SELECT * FROM game_features;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (game_date_et,))
        return cur.fetchall()

# -------------------------------------------------------------------
# Main Backtest Loop
# -------------------------------------------------------------------
def main():
    print("\n--- Running v2.1 Backtest (Last 7 Days) ---\n")

    try:
        win_pipe = joblib.load(MODEL_WIN_PATH)
        margin_model = joblib.load(MODEL_MARGIN_PATH)
        total_model = joblib.load(MODEL_TOTAL_PATH)
    except FileNotFoundError:
        print("Error: Models not found.")
        sys.exit(1)

    conn = get_db_conn()
    
    # Generate list of last 7 dates (excluding today/tomorrow)
    today = datetime.now()
    dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 8)]
    
    results = []

    feature_cols = [
        "rolling_pd_5_diff", "rolling_pd_10_diff", "rest_days_diff",
        "home_back_to_back", "home_advantage", "sos_diff"
    ]

    for d in dates:
        games = fetch_games_for_backtest(conn, d)
        if not games:
            continue
            
        for g in games:
            # Predict
            X_dict = {k: (float(g[k]) if g[k] is not None else 0.0) for k in feature_cols}
            X_df = pd.DataFrame([X_dict])
            
            pred_total = float(total_model.predict(X_df)[0])
            pred_margin = float(margin_model.predict(X_df)[0])
            
            pred_h = (pred_total + pred_margin) / 2
            pred_a = (pred_total - pred_margin) / 2
            
            # Grade
            actual_h = g['actual_home']
            actual_a = g['actual_away']
            
            # Did we pick the winner?
            pred_win_h = pred_h > pred_a
            actual_win_h = actual_h > actual_a
            correct = (pred_win_h == actual_win_h)
            
            # Error
            err_total = abs((pred_h + pred_a) - (actual_h + actual_a))
            err_margin = abs((pred_h - pred_a) - (actual_h - actual_a))

            results.append({
                'Date': d,
                'Matchup': f"{g['home_team']} vs {g['away_team']}",
                'Pred': f"{int(pred_h)}-{int(pred_a)}",
                'Actual': f"{actual_h}-{actual_a}",
                'Correct': correct,
                'Total_Err': err_total,
                'Margin_Err': err_margin
            })

    conn.close()
    
    if not results:
        print("No completed games found in the last 7 days to backtest.")
        return

    # Results Analysis
    df = pd.DataFrame(results)
    
    accuracy = df['Correct'].mean() * 100
    mae_total = df['Total_Err'].mean()
    mae_margin = df['Margin_Err'].mean()
    
    print(f"Games Simulated: {len(df)}")
    print(f"Win Accuracy:    {accuracy:.1f}%")
    print(f"Avg Margin Error: {mae_margin:.1f} pts")
    print(f"Avg Total Error:  {mae_total:.1f} pts")
    print("-" * 60)
    
    print("Recent Performance Log:")
    print(f"{'DATE':<12} | {'MATCHUP':<12} | {'PRED':<9} | {'ACTUAL':<9} | {'RESULT'}")
    print("-" * 60)
    
    # Show last 10 games
    for _, row in df.head(10).iterrows():
        icon = "✅" if row['Correct'] else "❌"
        print(f"{row['Date']:<12} | {row['Matchup']:<12} | {row['Pred']:<9} | {row['Actual']:<9} | {icon}")

if __name__ == "__main__":
    main()