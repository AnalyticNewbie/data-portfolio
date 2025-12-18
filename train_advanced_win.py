import os
import sys
import joblib
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

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

def fetch_training_data():
    conn = get_db_conn()
    
    # query joins the advanced stats views
    sql = """
        SELECT 
            g.game_id, 
            EXTRACT(DOW FROM g.game_date_et) as day_of_week, 
            
            -- Basic Features
            (hf.rolling_pd_5 - af.rolling_pd_5) as rolling_pd_5_diff,
            (hf.rolling_pd_10 - af.rolling_pd_10) as rolling_pd_10_diff,
            (hf.rest_days - af.rest_days) as rest_days_diff,
            CASE WHEN hf.rest_days = 0 THEN 1 ELSE 0 END as home_back_to_back,
            1 as home_advantage,
            (COALESCE(hsos.rolling_sos_10, 0) - COALESCE(asos.rolling_sos_10, 0)) as sos_diff,

            -- Advanced Features (Pace & Glass)
            -- Pace: Average of both teams' rolling pace (Game speed)
            (COALESCE(hadv.rolling_pace_10, 98.0) + COALESCE(aadv.rolling_pace_10, 98.0)) / 2.0 as pace_metric,
            
            -- Glass: Home Offensive Reb% vs Away Defensive Reb%
            -- (Note: Using simple proxy if columns are raw vals, or direct if metrics)
            -- Assuming rolling_oreb_10 is a rate or count. 
            (COALESCE(hadv.rolling_oreb_10, 0) - COALESCE(aadv.rolling_dreb_10, 0)) as home_glass_advantage,
            
            CASE WHEN g.home_pts > g.away_pts THEN 1 ELSE 0 END as home_win
            
        FROM games g
        JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
        LEFT JOIN v_strength_of_schedule asos ON g.away_team_id = asos.team_id AND g.game_id = asos.game_id
        -- New Joins
        LEFT JOIN v_team_advanced_stats hadv ON g.home_team_id = hadv.team_id AND g.game_id = hadv.game_id
        LEFT JOIN v_team_advanced_stats aadv ON g.away_team_id = aadv.team_id AND g.game_id = aadv.game_id
        
        WHERE g.status = 'Final'
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

def train_model():
    print("Fetching ADVANCED training data...")
    df = fetch_training_data()
    print(f"Loaded {len(df)} games.")

    feature_cols = [
        "rolling_pd_5_diff", 
        "rolling_pd_10_diff", 
        "rest_days_diff", 
        "home_back_to_back", 
        "home_advantage", 
        "sos_diff",
        "day_of_week",
        "pace_metric",          # New
        "home_glass_advantage"  # New
    ]
    
    X = df[feature_cols]
    y = df['home_win']
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # Update Pipeline to handle potential missing values in advanced stats
    pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')), # Fill missing advanced stats with average
        ('scaler', StandardScaler()),
        ('logreg', LogisticRegression(class_weight='balanced', solver='liblinear'))
    ])
    
    print("Training Advanced Model...")
    pipe.fit(X_train, y_train)
    
    # Evaluate
    acc = accuracy_score(y_test, pipe.predict(X_test))
    print(f"Model Accuracy: {acc:.1%}")
    
    os.makedirs("models", exist_ok=True)
    joblib.dump(pipe, "models/win_model.joblib") # Overwrite the old one
    print("Saved to models/win_model.joblib")

if __name__ == "__main__":
    train_model()