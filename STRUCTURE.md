# Project Directory Structure

Generated on: 2026-02-23 16:58:52

```
crick/
├── data/
│   └── stageddata/
│       ├── deliveries/
│       │   ├── MODI/
│       │   │   ├── chunk_0.parquet
│       │   │   ├── chunk_1.parquet
│       │   │   └── (4 more .parquet files)
│       │   ├── WODI/
│       │   │   ├── chunk_0.parquet
│       │   │   └── chunk_1.parquet
│       │   └── WT20I/
│       │       ├── chunk_0.parquet
│       │       ├── chunk_1.parquet
│       │       └── (2 more .parquet files)
│       ├── matches/
│       │   ├── MODI/
│       │   │   ├── chunk_0.parquet
│       │   │   ├── chunk_1.parquet
│       │   │   └── (4 more .parquet files)
│       │   ├── WODI/
│       │   │   ├── chunk_0.parquet
│       │   │   └── chunk_1.parquet
│       │   └── WT20I/
│       │       ├── chunk_0.parquet
│       │       ├── chunk_1.parquet
│       │       └── (2 more .parquet files)
│       ├── peoplematchdata/
│       │   ├── MODI/
│       │   │   ├── chunk_0.parquet
│       │   │   ├── chunk_1.parquet
│       │   │   └── (3 more .parquet files)
│       │   ├── WODI/
│       │   │   └── chunk_0.parquet
│       │   └── WT20I/
│       │       ├── chunk_0.parquet
│       │       ├── chunk_1.parquet
│       │       └── (1 more .parquet files)
│       ├── registry.db
│       ├── leagues.parquet
│       ├── players.parquet
│       └── (2 more .parquet files)
├── dbtcrick/
│   ├── models/
│   │   ├── inter/
│   │   │   └── intdeliverycont.sql
│   │   ├── mart/
│   │   │   └── batterstats1.sql
│   │   └── staging/
│   │       ├── sources.yml
│   │       ├── stg_deliveries.sql
│   │       └── stg_matches.sql
│   ├── dbt_project.yml
│   └── schema.yml
├── logs/
│   ├── download.log
│   ├── error_matches.log
│   └── (4 more .log files)
├── src/
│   ├── downloadespn.py
│   ├── extractleagues.py
│   ├── extractmatches.py
│   ├── extractplayers.py
│   ├── extractteams.py
│   ├── generate_structure.py
│   └── metaregis.py
├── .gitignore
├── Makefile
├── readme.md
├── requirements.txt
└── STRUCTURE.md
```
