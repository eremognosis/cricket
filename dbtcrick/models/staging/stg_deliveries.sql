{{ config(materialized='view') }}

SELECT * FROM {{ source('raw_cricket', 'deliveries') }}