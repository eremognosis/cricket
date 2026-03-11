
  
  create view "dev"."main"."stg_peoplematchdata__dbt_tmp" as (
    

SELECT * FROM '../data/stageddata/peoplematchdata/*/*.parquet'
  );
