# Project Directory Structure

Generated on: 2026-02-22 15:13:15

```
crick/
├── data/
│   └── stageddata/
│       ├── leagues.parquet
│       ├── players.parquet
│       ├── registry.db
│       ├── seasons.parquet
│       └── teams.parquet
├── dbtcrick/
│   ├── models/
│   │   ├── inter/
│   │   ├── mart/
│   │   └── staging/
│   ├── dbt_project.yml
│   └── schema.yml
├── logs/
│   ├── download.log
│   ├── error_players.log
│   ├── missing_players.log
│   └── parsing.log
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
