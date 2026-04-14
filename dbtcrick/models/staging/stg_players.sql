{{ config(materialized='view') }}

{% set players_path = '../data/stageddata/players.parquet' %}
{% set has_players = false %}

{% if execute %}
	{% set q = "select count(*) as c from glob('" ~ players_path ~ "')" %}
	{% set r = run_query(q) %}
	{% if r and (r.rows[0][0] | int) > 0 %}
		{% set has_players = true %}
	{% endif %}
{% endif %}

{% if has_players %}
SELECT * FROM read_parquet('{{ players_path }}')
{% else %}
SELECT
	CAST(NULL AS VARCHAR) AS player_id,
	CAST(NULL AS VARCHAR) AS cricinfo_id,
	CAST(NULL AS VARCHAR) AS name,
	CAST(NULL AS VARCHAR) AS full_name,
	CAST(NULL AS VARCHAR) AS dob,
	CAST(NULL AS VARCHAR) AS gender,
	CAST(NULL AS VARCHAR) AS batting_style,
	CAST(NULL AS VARCHAR) AS bowling_style,
	CAST(NULL AS VARCHAR) AS country_id,
	CAST(NULL AS VARCHAR) AS role
WHERE 1 = 0
{% endif %}
