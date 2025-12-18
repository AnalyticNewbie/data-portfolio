from db_config import get_db_connection

def add_rebound_columns():
    # 1. Get the connection using your centralized config
    conn = get_db_connection()
    
    # Safety check: ensure connection worked before proceeding
    if not conn:
        print("Failed to connect to database. Check .env and db_config.py")
        return

    print("Adding Rebound Columns...")
    
    try:
        with conn.cursor() as cur:
            # 2. Run the Schema Updates
            # Note: IF NOT EXISTS is valid in Postgres to prevent errors if you run this twice
            cur.execute("ALTER TABLE team_game_stats ADD COLUMN IF NOT EXISTS oreb_pct FLOAT DEFAULT 0.0;")
            cur.execute("ALTER TABLE team_game_stats ADD COLUMN IF NOT EXISTS dreb_pct FLOAT DEFAULT 0.0;")
            
            conn.commit()
            print("Columns added successfully.")
            
    except Exception as e:
        # Rollback in case of error to keep the transaction clean
        conn.rollback()
        print(f"Error adding columns: {e}")
        
    finally:
        # 3. Always close the connection
        conn.close()

if __name__ == "__main__":
    add_rebound_columns()