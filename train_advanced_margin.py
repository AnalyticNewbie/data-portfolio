import os
import sys
import joblib
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

# Load Environment Variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

if not DB_PASS:
    print("Error: DB_PASSWORD not found.")
    sys.exit(1)

def get_db_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def train():
    conn = get_db_conn()
    
    # Query must match the Win Model features exactly + Target (Margin)
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
            (COALESCE(hadv.rolling_pace_10, 98.0) + COALESCE(aadv.rolling_pace_10, 98.0)) / 2.0 as pace_metric,
            (COALESCE(hadv.rolling_oreb_10, 0) - COALESCE(aadv.rolling_dreb_10, 0)) as home_glass_advantage,
            
            -- TARGET: Margin (Home - Away)
            (g.home_pts - g.away_pts) as margin
            
        FROM games g
        JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
        LEFT JOIN v_strength_of_schedule asos ON g.away_team_id = asos.team_id AND g.game_id = asos.game_id
        LEFT JOIN v_team_advanced_stats hadv ON g.home_team_id = hadv.team_id AND g.game_id = hadv.game_id
        LEFT JOIN v_team_advanced_stats aadv ON g.away_team_id = aadv.team_id AND g.game_id = aadv.game_id
        WHERE g.status = 'Final'
    """
    
    try:
        df = pd.read_sql(sql, conn)
    except Exception as e:
        print(f"Error fetching data: {e}")
        sys.exit(1)
    finally:
        conn.close()

    features = [
        "rolling_pd_5_diff", "rolling_pd_10_diff", "rest_days_diff", 
        "home_back_to_back", "home_advantage", "sos_diff", 
        "day_of_week", "pace_metric", "home_glass_advantage"
    ]
    
    X = df[features]
    y = df['margin']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler()),
        ('ridge', Ridge(alpha=1.0))
    ])
    
    print("Training Advanced Margin Model...")
    pipe.fit(X_train, y_train)
    
    mae = mean_absolute_error(y_test, pipe.predict(X_test))
    print(f"Margin MAE: {mae:.2f} points")
    
    os.makedirs("models", exist_ok=True)
    joblib.dump(pipe, "models/margin_model.joblib")
    print("Saved models/margin_model.joblib")

if __name__ == "__main__":
    train()