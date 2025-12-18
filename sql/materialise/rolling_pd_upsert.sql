BEGIN;

INSERT INTO features_team_game (
    game_id,
    team_id,
    as_of_date,
    feature_json
)
SELECT
    v.game_id,
    v.team_id,
    v.scheduled_date_et AS as_of_date,
    jsonb_build_object(
        'rolling_pd_5', v.rolling_pd_5,
        'rolling_pd_5_n', v.rolling_pd_5_n,
        'rolling_pd_5_missing', (v.rolling_pd_5_n < 5),
        'rolling_pd_10', v.rolling_pd_10,
        'rolling_pd_10_n', v.rolling_pd_10_n,
        'rolling_pd_10_missing', (v.rolling_pd_10_n < 10)
    ) AS feature_json
FROM v_rolling_form_scheduled v
ON CONFLICT (game_id, team_id) DO UPDATE
SET
    as_of_date = EXCLUDED.as_of_date,
    feature_json = COALESCE(features_team_game.feature_json, '{}'::jsonb) || EXCLUDED.feature_json;

COMMIT;
