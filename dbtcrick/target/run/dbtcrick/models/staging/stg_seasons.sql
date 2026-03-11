
  
  create view "dev"."main"."stg_seasons__dbt_tmp" as (
    

SELECT * FROM '../data/stageddata/seasons.parquet'
  );
