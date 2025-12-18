-- Core reference tables
CREATE TABLE IF NOT EXISTS teams (
  team_id         INTEGER PRIMARY KEY,
  team_abbr       TEXT,
  team_name       TEXT
);

CREATE TABLE IF NOT EXISTS games (
  game_id         TEXT PRIMARY KEY,
  game_date       DATE NOT NULL,
  season          INTEGER,
  home_team_id    INTEGER NOT NULL,
  away_team_id    INTEGER NOT NULL,
  status          TEXT NOT NULL,   -- scheduled | in_progress | final
  home_pts        INTEGER,
  away_pts        INTEGER,
  updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_home ON games(home_team_id);
CREATE INDEX IF NOT EXISTS idx_games_away ON games(away_team_id);

-- One row per team per game (box-score-derived team stats you compute)
CREATE TABLE IF NOT EXISTS team_game_stats (
  game_id         TEXT NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,
  team_id         INTEGER NOT NULL,
  is_home         BOOLEAN NOT NULL,
  pts             INTEGER,
  opp_pts         INTEGER,

  -- simple derived quantities (you can expand later)
  poss_est        NUMERIC,         -- estimated possessions
  ortg            NUMERIC,
  drtg            NUMERIC,
  netrtg          NUMERIC,
  pace_est        NUMERIC,

  -- Four Factors style fields (optional now, handy later)
  efg_pct         NUMERIC,
  tov_pct         NUMERIC,
  orb_pct         NUMERIC,
  ftr             NUMERIC,

  PRIMARY KEY (game_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_tgs_team ON team_game_stats(team_id);

-- Model-ready features per team-game (rolling / lagged features)
CREATE TABLE IF NOT EXISTS features_team_game (
  game_id         TEXT NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,
  team_id         INTEGER NOT NULL,
  as_of_date      DATE NOT NULL,   -- the date features are valid "as of" (typically game_date)
  feature_json    JSONB NOT NULL,  -- flexible for v1; can normalize later
  PRIMARY KEY (game_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_ftg_asof ON features_team_game(as_of_date);

-- Predictions per game per run
CREATE TABLE IF NOT EXISTS predictions (
  prediction_id   BIGSERIAL PRIMARY KEY,
  run_date        DATE NOT NULL,
  game_id         TEXT NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,
  model_version   TEXT NOT NULL,

  p_home_win      NUMERIC NOT NULL,

  home_median     NUMERIC,
  away_median     NUMERIC,
  home_p05        NUMERIC,
  home_p95        NUMERIC,
  away_p05        NUMERIC,
  away_p95        NUMERIC,

  total_median    NUMERIC,
  total_p05       NUMERIC,
  total_p95       NUMERIC,
  margin_median   NUMERIC,
  margin_p05      NUMERIC,
  margin_p95      NUMERIC,

  created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pred_run_date ON predictions(run_date);
CREATE INDEX IF NOT EXISTS idx_pred_game ON predictions(game_id);

-- Evaluation metrics (for tracking calibration, coverage, etc.)
CREATE TABLE IF NOT EXISTS evaluation_runs (
  eval_id         BIGSERIAL PRIMARY KEY,
  run_date        DATE NOT NULL,
  model_version   TEXT NOT NULL,
  sample_start    DATE,
  sample_end      DATE,
  log_loss        NUMERIC,
  brier           NUMERIC,
  score_cover_90  NUMERIC,
  score_cover_95  NUMERIC,
  avg_width_95    NUMERIC,
  created_at      TIMESTAMPTZ DEFAULT now()
);
