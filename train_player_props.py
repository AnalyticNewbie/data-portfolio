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

load_dotenv()
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

def train_prop_model(target_col, model_name):
    print(f"\n--- Training {model_name} Model ---")
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    
    # We select specific columns. 
    # Note: Postgres returns these as lowercase (pts_l5, etc.)
    sql = f"""
        SELECT 
            pts_l5, reb_l5, ast_l5, min_l5,
            pts_l10, reb_l10, ast_l10,
            pts_l20,
            {target_col} as target
        FROM v_player_rolling_stats
        WHERE pts_l10 IS NOT NULL -- Ensure we have enough history
    """
    
    try:
        df = pd.read_sql(sql, conn)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return
    finally:
        conn.close()
    
    # FIXED: Feature list must be all lowercase to match Postgres output
    features = [
        "pts_l5", "reb_l5", "ast_l5", "min_l5",
        "pts_l10", "reb_l10", "ast_l10", "pts_l20"
    ]
    
    # Check if data exists
    if df.empty:
        print("Warning: No data found. Make sure you ran the SQL to create 'v_player_rolling_stats'.")
        return

    X = df[features]
    y = df['target']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler()),
        ('ridge', Ridge(alpha=1.0))
    ])
    
    pipe.fit(X_train, y_train)
    
    mae = mean_absolute_error(y_test, pipe.predict(X_test))
    print(f"{model_name} MAE: {mae:.2f}")
    
    os.makedirs("models", exist_ok=True)
    joblib.dump(pipe, f"models/{model_name.lower()}_model.joblib")
    print(f"Saved models/{model_name.lower()}_model.joblib")

if __name__ == "__main__":
    train_prop_model("actual_pts", "Points")
    train_prop_model("actual_reb", "Rebounds")
    train_prop_model("actual_ast", "Assists")