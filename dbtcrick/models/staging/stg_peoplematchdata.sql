{{ config(materialized='view') }}

SELECT * FROM read_parquet('../data/stageddata/peoplematchdata/*/*.parquet')
