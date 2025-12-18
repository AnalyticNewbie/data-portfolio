import sys
import os
import joblib
import pandas as pd
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

# Config
DB_HOST = "localhost"
DB_NAME = "nba"
DB_USER = "nba_user"
DB_PASS = "nba_pass"

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def fetch_data():
    conn = get_db_conn()
    sql = """
        SELECT 
            g.game_id,
            EXTRACT(DOW FROM g.game_date_et) as day_of_week,
            
            (hf.rolling_pd_5 - af.rolling_pd_5) as rolling_pd_5_diff,
            (hf.rolling_pd_10 - af.rolling_pd_10) as rolling_pd_10_diff,
            (hf.rest_days - af.rest_days) as rest_days_diff,
            CASE WHEN hf.rest_days = 0 THEN 1 ELSE 0 END as home_back_to_back,
            1 as home_advantage,
            (COALESCE(hsos.rolling_sos_10, 0) - COALESCE(asos.rolling_sos_10, 0)) as sos_diff,
            
            -- PACE (V4.0)
            (COALESCE(hadv.rolling_pace_10, 100) + COALESCE(aadv.rolling_pace_10, 100)) / 2.0 as pace_metric,
            
            -- REBOUNDING (V4.1 - The "Second Chance" Factor)
            -- High Score = Home gets many offensive boards (high efficiency putbacks)
            (COALESCE(hadv.rolling_oreb_10, 0.25) - COALESCE(aadv.rolling_dreb_10, 0.75)) as home_glass_advantage,
            
            (g.home_pts - g.away_pts) as margin,
            (g.home_pts + g.away_pts) as total_points
            
        FROM games g
        JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
        LEFT JOIN v_strength_of_schedule asos ON g.away_team_id = asos.team_id AND g.game_id = asos.game_id
        LEFT JOIN v_team_advanced_stats hadv ON g.home_team_id = hadv.team_id AND g.game_id = hadv.game_id
        LEFT JOIN v_team_advanced_stats aadv ON g.away_team_id = aadv.team_id AND g.game_id = aadv.game_id
        
        WHERE g.status = 'Final'
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

def train_score_models():
    print("Loading V4.1 Data (Pace + Rebounding)...")
    df = fetch_data()
    print(f"Loaded {len(df)} games.")
    
    feature_cols = [
        "rolling_pd_5_diff", 
        "rolling_pd_10_diff", 
        "rest_days_diff", 
        "home_back_to_back", 
        "home_advantage", 
        "sos_diff", 
        "day_of_week",
        "pace_metric",
        "home_glass_advantage" # <--- NEW FEATURE
    ]
    
    X = df[feature_cols]
    y_margin = df['margin']
    y_total = df['total_points']
    
    print("\nTraining Margin Model...")
    model_margin = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=3, random_state=42)
    model_margin.fit(X, y_margin)
    
    print("Training Total Points Model...")
    model_total = GradientBoostingRegressor(n_estimators=150, learning_rate=0.1, max_depth=4, random_state=42)
    model_total.fit(X, y_total)
    
    # Check Accuracy
    total_preds = model_total.predict(X)
    mae = mean_absolute_error(y_total, total_preds)
    print(f"Total Points MAE: {mae:.2f} pts")
    
    os.makedirs("models", exist_ok=True)
    joblib.dump(model_margin, "models/margin_model.joblib")
    joblib.dump(model_total, "models/total_model.joblib")
    print("Models saved.")

if __name__ == "__main__":
    train_score_models()