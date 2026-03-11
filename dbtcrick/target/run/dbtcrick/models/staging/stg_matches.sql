
  
  create view "dev"."main"."stg_matches__dbt_tmp" as (
    

SELECT * FROM '../data/stageddata/matches/*/*.parquet'
  );
