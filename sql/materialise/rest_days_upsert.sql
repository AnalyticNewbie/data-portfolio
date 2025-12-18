BEGIN;

-- 1) Ensure the base feature row exists for every scheduled team-game
INSERT INTO features_team_game (game_id, team_id, as_of_date, feature_json)
SELECT
    v.game_id,
    v.team_id,
    v.scheduled_date_et AS as_of_date,
    '{}'::jsonb AS feature_json
FROM public.v_rest_days_scheduled v
ON CONFLICT (game_id, team_id)
DO UPDATE SET
    as_of_date = EXCLUDED.as_of_date,
    feature_json = COALESCE(features_team_game.feature_json, '{}'::jsonb);

-- 2) Update rest day fields
UPDATE features_team_game f
SET
    rest_days = v.rest_days,
    rest_days_missing = v.rest_days_missing
FROM public.v_rest_days_scheduled v
WHERE f.game_id = v.game_id
  AND f.team_id = v.team_id;

COMMIT;
