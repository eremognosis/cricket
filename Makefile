.PHONY: run_pipeline extract transform clean


all: extract transform

extract:
	pip3 install -r requirements.txt
	python src/getleaguefiles.py
	python src/downloadespn.py
	python src/downloadpleyrs.py
	python src/extractleagues.py
	python src/extractteams.py
	python src/extractplayers.py
	python src/extractmatches.py


transform:
	cd dbtcrick && dbt build

# Nuke the staged data and DuckDB file to start fresh
clean:
	rm -rf data/rawdata/playerjsons/*.json
	rm -rf data/stageddata/*.parquet
	rm -f data/stageddata/crick.duckdb