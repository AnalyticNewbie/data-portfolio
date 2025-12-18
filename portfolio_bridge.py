# portfolio_bridge.py - SAFE: Only reads data and writes JSON
import json
from datetime import datetime

def generate_site_data():
    # 1. Provide the factual benchmarks we discussed
    manifest = {
        "last_updated": datetime.now().strftime("%Y-%m-%d"),
        "metrics": {
            "accuracy": "57.6%",
            "mae": "10.13"
        },
        "matchups": [], # This will be populated by your team prediction output
        "props": []     # This will be populated by your prop prediction output
    }

    # 2. Save the file for the website
    with open("data.json", "w") as f:
        json.dump(manifest, f, indent=4)
    print("Portfolio data synchronized successfully.")

if __name__ == "__main__":
    generate_site_data()