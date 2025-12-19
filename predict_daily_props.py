import json
import os

def generate_top_insights(all_projections):
    """
    Filters and sorts all player projections to find the top 5 'High Confidence' edges.
    """
    insights = []
    
    for p in all_projections:
        # Calculate 'Edge' percentage vs. their recent average
        # Using PTS as the primary filter for this example
        pts_diff = abs(p['proj_pts'] - p['avg_pts'])
        edge_pct = pts_diff / p['avg_pts'] if p['avg_pts'] > 0 else 0
        
        # Only consider players with significant playing time (>20 mins avg)
        if p['avg_min'] > 20:
            insights.append({
                "name": p['name'],
                "proj_pts": round(p['proj_pts'], 1),
                "proj_reb": round(p['proj_reb'], 1),
                "proj_ast": round(p['proj_ast'], 1),
                "away_abbr": p['away_abbr'],
                "home_abbr": p['home_abbr'],
                "edge_score": edge_pct,
                "edge_type": "High" if edge_pct > 0.15 else "Normal"
            })

    # Sort by the highest Edge Score and take the top 5
    top_5 = sorted(insights, key=lambda x: x['edge_score'], reverse=True)[:5]
    return top_5

# --- DATA EXPORT ---
# This part runs at the very end of your script
top_5_insights = generate_top_insights(all_calculated_player_data)

json_path = "projects/nba-predictor/data.json"
output_data = {
    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "props": top_5_insights
}

with open(json_path, "w") as f:
    json.dump(output_data, f, indent=4)

print(f"✅ Successfully exported top {len(top_5_insights)} insights to {json_path}")