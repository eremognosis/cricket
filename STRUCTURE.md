# Project Directory Structure

Generated on: 2026-04-14 11:09:16

```
/
├── STRUCTURE.md
├── readme.md
├── requirements.txt
├── .gitignore
├── Makefile
├── data/
│   ├── rawdata/
│   │   ├── ipl_json.zip
│   │   ├── wbb_female_json.zip
│   │   ├── leaguejsons/
│   │   │   ├── 1001201261.json
│   │   │   ├── 1002874445.json
│   │   │   └── (2148 more .json files)
│   │   ├── playerjsons/
│   │   │   ├── 1000612344.json
│   │   │   ├── 1003023775.json
│   │   │   └── (940 more .json files)
│   │   ├── registry/
│   │   │   └── people.csv
│   │   ├── seasons/
│   │   │   └── 8048/
│   │   │       ├── 2008.json
│   │   │       ├── 2009.json
│   │   │       └── (17 more .json files)
│   │   └── teamjsons/
│   │       ├── 1000996286.json
│   │       ├── 100166831.json
│   │       └── (6563 more .json files)
│   └── stageddata/
│       ├── registry.db
│       ├── playeridmap.parquet
│       ├── teams.parquet
│       ├── deliveries/
│       │   └── IPL/
│       │       ├── chunk_0.parquet
│       │       ├── chunk_1.parquet
│       │       └── (1 more .parquet file)
│       ├── matches/
│       │   └── IPL/
│       │       ├── chunk_0.parquet
│       │       ├── chunk_1.parquet
│       │       └── (1 more .parquet file)
│       └── peoplematchdata/
│           └── IPL/
│               ├── chunk_0.parquet
│               ├── chunk_1.parquet
│               └── (1 more .parquet file)
├── dbtcrick/
│   ├── dev.duckdb
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── schema.yml
│   ├── .user.yml
│   ├── .vscode/
│   │   └── extensions.json
│   ├── logs/
│   │   ├── dbt.log
│   │   └── query_log.sql
│   └── models/
│       ├── inter/
│       │   └── intdeliverycont.sql
│       ├── mart/
│       │   ├── batterstats.sql
│       │   └── bowlerstats.sql
│       └── staging/
│           ├── stg_deliveries.sql
│           ├── stg_leagues.sql
│           └── (6 more .sql files)
├── logs/
│   ├── dbt.log
│   ├── download.log
│   ├── error_players.log
│   ├── missing_players.log
│   └── query_log.sql
└── src/
    ├── bidmap.py
    ├── downloadespn.py
    ├── downloadpleyrs.py
    ├── extractleagues.py
    ├── extractmatches.py
    ├── extractplayers.py
    ├── extractteams.py
    ├── generate_structure.py
    ├── getleaguefiles.py
    ├── getteams.py
    └── metaregis.py
```
