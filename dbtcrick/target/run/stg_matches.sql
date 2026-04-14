
  
  create view "dev"."main"."stg_matches__dbt_tmp" as (
    

SELECT * FROM read_parquet('../data/stageddata/matches/*/*.parquet')
  );
