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

# 1. Load Environment Variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nba")
DB_USER = os.getenv("DB_USER", "nba_user")
DB_PASS = os.getenv("DB_PASSWORD")

# Safety Check
if not DB_PASS:
    print("Error: DB_PASSWORD not found. Please check your .env file.")
    sys.exit(1)

def get_db_conn():
    """Establish connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, 
            dbname=DB_NAME, 
            user=DB_USER, 
            password=DB_PASS
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)

def fetch_training_data():
    """Fetch feature-engineered data from SQL Views"""
    conn = get_db_conn()
    
    # Query uses the views v_team_features_with_rest and v_strength_of_schedule
    # Note: Using 'game_date_et' as confirmed in your database schema
    sql = """
        SELECT 
            g.game_id, 
            EXTRACT(DOW FROM g.game_date_et) as day_of_week, 
            
            -- Feature: Rolling Point Differential Diff (Home - Away)
            (hf.rolling_pd_5 - af.rolling_pd_5) as rolling_pd_5_diff,
            (hf.rolling_pd_10 - af.rolling_pd_10) as rolling_pd_10_diff,
            
            -- Feature: Rest Days Diff (Positive = Home is more rested)
            (hf.rest_days - af.rest_days) as rest_days_diff,
            
            -- Feature: Fatigue Flag
            CASE WHEN hf.rest_days = 0 THEN 1 ELSE 0 END as home_back_to_back,
            
            -- Feature: Home Court (Static 1, but useful for intercept scaling)
            1 as home_advantage,
            
            -- Feature: SOS Diff (Did Home team play harder opponents recently?)
            (COALESCE(hsos.rolling_sos_10, 0) - COALESCE(asos.rolling_sos_10, 0)) as sos_diff,
            
            -- Target: 1 if Home won, 0 if Away won
            CASE WHEN g.home_pts > g.away_pts THEN 1 ELSE 0 END as home_win
            
        FROM games g
        JOIN v_team_features_with_rest hf ON g.home_team_id = hf.team_id AND g.game_id = hf.game_id
        JOIN v_team_features_with_rest af ON g.away_team_id = af.team_id AND g.game_id = af.game_id
        LEFT JOIN v_strength_of_schedule hsos ON g.home_team_id = hsos.team_id AND g.game_id = hsos.game_id
        LEFT JOIN v_strength_of_schedule asos ON g.away_team_id = asos.team_id AND g.game_id = asos.game_id
        WHERE g.status = 'Final'
    """
    
    try:
        df = pd.read_sql(sql, conn)
    except Exception as e:
        print(f"SQL Query Failed: {e}")
        sys.exit(1)
    finally:
        conn.close()
        
    return df

def train_model():
    print("--- Starting Training Pipeline ---")
    
    # 1. Get Data
    df = fetch_training_data()
    print(f"Loaded {len(df)} games for training.")

    # 2. Define Features
    feature_cols = [
        "rolling_pd_5_diff", 
        "rolling_pd_10_diff", 
        "rest_days_diff", 
        "home_back_to_back", 
        "home_advantage", 
        "sos_diff",
        "day_of_week"
    ]
    
    # Handle NaN (Early season games might have missing rolling averages)
    X = df[feature_cols].fillna(0)
    y = df['home_win']
    
    # 3. Split Data
    # Stratify ensures we keep the same win/loss ratio in train and test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 4. Build Pipeline
    # StandardScaler: Normalizes data (e.g., Rest Days vs Point Diff have different scales)
    # LogisticRegression: 'balanced' mode automatically adjusts for home field advantage bias
    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('logreg', LogisticRegression(class_weight='balanced', solver='liblinear'))
    ])
    
    print("Training Logistic Regression Model...")
    pipe.fit(X_train, y_train)
    
    # 5. Evaluate
    probs = pipe.predict_proba(X_test)[:, 1]
    preds = pipe.predict(X_test)
    
    auc = roc_auc_score(y_test, probs)
    loss = log_loss(y_test, probs)
    acc = accuracy_score(y_test, preds)
    
    print("\n=== Model Results ===")
    print(f"Accuracy:  {acc:.2%}")
    print(f"AUC Score: {auc:.4f}")
    print(f"Log Loss:  {loss:.4f}")
    
    # 6. Save Model
    os.makedirs("models", exist_ok=True)
    model_path = "models/win_model.joblib"
    joblib.dump(pipe, model_path)
    print(f"\nSUCCESS: Model saved to {model_path}")

if __name__ == "__main__":
    train_model()