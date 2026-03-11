
  
  create view "dev"."main"."stg_playeridmap__dbt_tmp" as (
    

SELECT * FROM '../data/stageddata/playeridmap.parquet'
  );
