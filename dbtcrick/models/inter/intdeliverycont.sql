{{ config(materialized='table') }}

WITH base AS (
	SELECT
		d.*, 
		row_number() OVER (
			PARTITION BY d.matchid, d.inning
			ORDER BY d.over_num, d.ball_num
		) AS delivery_seq,
		coalesce(d.is_legal_ball, 0) AS legal_ball_flag,
		coalesce(d.is_wicket, 0) AS wicket_flag,
		coalesce(d.is_boundary, 0) AS boundary_flag,
		coalesce(d.runs_batter_thisball, 0) AS batter_runs_ball,
		coalesce(d.runs_total_thisball, 0) AS total_runs_ball
	FROM {{ ref('stg_deliveries') }} d
),

team_progress AS (
	SELECT
		b.*,
		sum(b.total_runs_ball) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS innings_runs_to_ball,
		sum(b.wicket_flag) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS innings_wickets_to_ball,
		sum(b.legal_ball_flag) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS innings_legal_balls_to_ball,
		sum(b.total_runs_ball) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
		) AS team_runs_last_5_balls,
		sum(b.wicket_flag) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
		) AS team_wickets_last_5_balls,
		sum(b.boundary_flag) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
		) AS team_boundaries_last_5_balls,
		sum(b.total_runs_ball) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
		) AS team_runs_last_10_balls,
		sum(b.wicket_flag) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
		) AS team_wickets_last_10_balls,
		sum(b.boundary_flag) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
		) AS team_boundaries_last_10_balls,
		sum(b.legal_ball_flag) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
		) AS team_legal_balls_last_10,
		max(CASE WHEN b.boundary_flag = 1 THEN b.delivery_seq END) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS last_boundary_seq,
		max(CASE WHEN b.wicket_flag = 1 THEN b.delivery_seq END) OVER (
			PARTITION BY b.matchid, b.inning
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS last_wicket_seq
	FROM base b
),

batter_progress AS (
	SELECT
		b.matchid,
		b.inning,
		b.delivery_seq,
		b.batter_id AS player_id,
		sum(b.batter_runs_ball) OVER (
			PARTITION BY b.matchid, b.inning, b.batter_id
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS batter_runs_to_ball,
		sum(b.legal_ball_flag) OVER (
			PARTITION BY b.matchid, b.inning, b.batter_id
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS batter_balls_to_ball,
		sum(b.batter_runs_ball) OVER (
			PARTITION BY b.matchid, b.inning, b.batter_id
			ORDER BY b.delivery_seq
			ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
		) AS batter_runs_last_10_balls_faced,
		sum(b.legal_ball_flag) OVER (
			PARTITION BY b.matchid, b.inning, b.batter_id
			ORDER BY b.delivery_seq
			ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
		) AS batter_legal_balls_last_10_faced
	FROM base b
),

bowler_progress AS (
	SELECT
		b.matchid,
		b.inning,
		b.delivery_seq,
		b.bowler_id,
		sum(b.total_runs_ball) OVER (
			PARTITION BY b.matchid, b.inning, b.bowler_id
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS bowler_runs_conceded_to_ball,
		sum(b.legal_ball_flag) OVER (
			PARTITION BY b.matchid, b.inning, b.bowler_id
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS bowler_legal_balls_to_ball,
		sum(
			CASE
				WHEN b.wicket_flag = 1
					AND lower(coalesce(b.wicket_type, '')) NOT IN (
						'run out',
						'retired hurt',
						'retired out',
						'obstructing the field'
					)
				THEN 1
				ELSE 0
			END
		) OVER (
			PARTITION BY b.matchid, b.inning, b.bowler_id
			ORDER BY b.delivery_seq
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS bowler_wickets_to_ball
	FROM base b
),

inning_targets AS (
	SELECT
		t.*,
		max(CASE WHEN t.inning = 1 THEN t.innings_runs_to_ball END) OVER (
			PARTITION BY t.matchid
		) AS first_innings_final_runs,
		cast(max(coalesce(m.overs, 20)) OVER (PARTITION BY t.matchid) * 6 AS INTEGER) AS scheduled_legal_balls
	FROM team_progress t
	LEFT JOIN {{ ref('stg_matches') }} m
		ON t.matchid = m.matchid
)

SELECT
	i.matchid,
	i.inning,
	i.battingteam,
	i.over_num,
	i.ball_num,
	i.phase,
	i.batter_id,
	i.bowler_id,
	i.non_striker_id,
	i.runs_batter_thisball,
	i.runs_extras_thisball,
	i.runs_total_thisball,
	i.iswide,
	i.isNoball,
	i.is_boundary,
	i.is_dot,
	i.is_legal_ball,
	i.is_wicket,
	i.wicket_type,
	i.player_out_id,
	i.fielderct,
	i.isreview,
	i.review_by,
	i.review_umpire,
	i.review_batter_id,
	i.review_bowler_id,
	i.review_decision,
	i.review_type,

	i.innings_runs_to_ball AS curr_innings_runs,
	i.innings_wickets_to_ball AS curr_innings_wickets,
	i.innings_legal_balls_to_ball AS curr_innings_legal_balls,
	floor(i.innings_legal_balls_to_ball / 6)
		+ ((i.innings_legal_balls_to_ball % 6) / 10.0) AS curr_innings_overs,
	i.delivery_seq AS curr_innings_ball_seq,

	sp.batter_runs_to_ball AS striker_runs_to_ball,
	sp.batter_balls_to_ball AS striker_balls_to_ball,
	np.batter_runs_to_ball AS non_striker_runs_to_ball,
	np.batter_balls_to_ball AS non_striker_balls_to_ball,

	CASE
		WHEN i.innings_legal_balls_to_ball = 0 THEN NULL
		ELSE (i.innings_runs_to_ball * 6.0) / i.innings_legal_balls_to_ball
	END AS curr_run_rate,

	CASE
		WHEN i.inning <> 2 THEN NULL
		ELSE i.first_innings_final_runs + 1
	END AS target_runs,

	CASE
		WHEN i.inning <> 2 THEN NULL
		ELSE greatest((i.first_innings_final_runs + 1) - i.innings_runs_to_ball, 0)
	END AS runs_required,

	CASE
		WHEN i.inning <> 2 THEN NULL
		ELSE greatest(i.scheduled_legal_balls - i.innings_legal_balls_to_ball, 0)
	END AS legal_balls_remaining,

	CASE
		WHEN i.inning <> 2 THEN NULL
		WHEN greatest(i.scheduled_legal_balls - i.innings_legal_balls_to_ball, 0) = 0 THEN NULL
		ELSE (
			greatest((i.first_innings_final_runs + 1) - i.innings_runs_to_ball, 0) * 6.0
		) / greatest(i.scheduled_legal_balls - i.innings_legal_balls_to_ball, 0)
	END AS required_run_rate,

	bp.bowler_runs_conceded_to_ball AS bowler_runs_conceded_to_ball,
	bp.bowler_legal_balls_to_ball AS bowler_legal_balls_to_ball,
	floor(bp.bowler_legal_balls_to_ball / 6)
		+ ((bp.bowler_legal_balls_to_ball % 6) / 10.0) AS bowler_overs_to_ball,
	bp.bowler_wickets_to_ball AS bowler_wickets_to_ball,

	i.delivery_seq - i.last_boundary_seq AS balls_since_last_boundary,
	i.delivery_seq - i.last_wicket_seq AS balls_since_last_wicket,

	i.team_runs_last_5_balls,
	i.team_wickets_last_5_balls,
	i.team_boundaries_last_5_balls,
	i.team_runs_last_10_balls,
	i.team_wickets_last_10_balls,
	i.team_boundaries_last_10_balls,

	CASE
		WHEN i.team_legal_balls_last_10 = 0 THEN NULL
		ELSE (i.team_runs_last_10_balls * 6.0) / i.team_legal_balls_last_10
	END AS team_run_rate_last_10_balls,

	sp.batter_runs_last_10_balls_faced,
	sp.batter_legal_balls_last_10_faced,
	CASE
		WHEN sp.batter_legal_balls_last_10_faced = 0 THEN NULL
		ELSE (sp.batter_runs_last_10_balls_faced * 100.0) / sp.batter_legal_balls_last_10_faced
	END AS striker_sr_last_10_balls_faced

FROM inning_targets i
LEFT JOIN bowler_progress bp
	ON i.matchid = bp.matchid
	AND i.inning = bp.inning
	AND i.delivery_seq = bp.delivery_seq
	AND i.bowler_id = bp.bowler_id
LEFT JOIN LATERAL (
	SELECT
		p.batter_runs_to_ball,
		p.batter_balls_to_ball,
		p.batter_runs_last_10_balls_faced,
		p.batter_legal_balls_last_10_faced
	FROM batter_progress p
	WHERE p.matchid = i.matchid
	  AND p.inning = i.inning
	  AND p.player_id = i.batter_id
	  AND p.delivery_seq <= i.delivery_seq
	ORDER BY p.delivery_seq DESC
	LIMIT 1
) sp ON TRUE
LEFT JOIN LATERAL (
	SELECT
		p.batter_runs_to_ball,
		p.batter_balls_to_ball
	FROM batter_progress p
	WHERE p.matchid = i.matchid
	  AND p.inning = i.inning
	  AND p.player_id = i.non_striker_id
	  AND p.delivery_seq <= i.delivery_seq
	ORDER BY p.delivery_seq DESC
	LIMIT 1
) np ON TRUE