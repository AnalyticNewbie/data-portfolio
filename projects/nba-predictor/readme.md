# NBA Game Prediction System 🏀

An end-to-end Machine Learning pipeline that predicts NBA game outcomes, scores, and confidence intervals. The system has evolved from a simple statistical baseline to a context-aware volatility model running on PostgreSQL and Python.

## 🚀 Current Status: v2.1 (Context-Aware Volatility)

The system currently predicts:
1.  **Win Probability:** Using Weighted Logistic Regression.
2.  **Exact Scores:** Using Gradient Boosting Regressors (`HistGradientBoostingRegressor`).
3.  **Dynamic Uncertainty:** Confidence intervals that expand/contract based on game pace and matchup styles.
4.  **Risk Factors:** Automatic flags for "High OT Risk" and team momentum ("Heating Up" vs "Slumping").

---

## 🛠 Tech Stack

* **Core:** Python 3.13
* **Database:** PostgreSQL 16 (running via Docker)
* **Data Source:** `nba_api` (Official NBA stats)
* **ML Libraries:** `scikit-learn`, `joblib`, `scipy`
* **Engineering:** SQL Views, Window Functions, Materialized Logic

---

## 📈 System Evolution

### v2.1: Advanced Volatility & Risk (Current)
* **The Problem:** Standard ML models often output static confidence intervals (e.g., +/- 10 points) regardless of the opponent.
* **The Solution:** Implemented a **Composite Sigma** logic.
    * **Pace Factor:** Faster games = Wider prediction range.
    * **Defense Factor:** Elite defensive opponents "compress" the range.
    * **Form Watch:** A weighted "Last 3 Games" margin to identify short-term momentum shifts versus season-long averages.
* **SQL Update:** Rewrote views to anchor rolling stats to the full schedule, ensuring future games can look backward at historical data correctly.

### v2.0: The Machine Learning Shift
* **The Problem:** v1.0 couldn't account for complex non-linear factors like "Rest Disadvantage" or "Strength of Schedule."
* **The Solution:**
    * **SQL-First Features:** Moved feature engineering (Rolling Averages, SOS) into PostgreSQL Views for speed and consistency.
    * **The Truth Loop:** Created a `prediction_history` table to log every prediction and a script to automatically ingest actual scores and grade accuracy the next day.
    * **Models:** Switched to Gradient Boosting for score prediction.

### v1.0: The Baseline
* **Concept:** A Proof of Concept (MVP) using simple Z-Scores and Net Ratings.
* **Goal:** Establish the data ingestion pipeline and database connectivity.

---

## 📂 Project Structure

```text
├── models/                   # Serialized .joblib ML models
├── predict_scores_v2.1.py    # Main Daily Driver: Generates insights & risk flags
├── train_score_models.py     # Retrains the Score/Margin models (Gradient Boosting)
├── train_baseline_win.py     # Retrains the Win Probability model (Logistic Regression)
├── ingest_schedule.py        # Pulls upcoming games from NBA API
├── ingest_results_et.py      # Pulls actual scores & grades past predictions
└── README.md
