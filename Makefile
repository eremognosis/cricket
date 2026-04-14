.PHONY: run_pipeline extract transform clean pipeline

SEL ?=
LEAGUE_ID ?=


all: extract transform

extract:
	pip3 install -r requirements.txt
	python src/getleaguefiles.py $(if $(LEAGUE_ID),--league-id $(LEAGUE_ID),)
	python src/getteams.py
	python src/downloadespn.py $(if $(LEAGUE_ID),--league-id $(LEAGUE_ID),)
	python src/downloadpleyrs.py $(if $(SEL),--select-data $(SEL),)
	python src/extractleagues.py
	python src/extractteams.py
	@if [ -n "$(SEL)" ]; then \
		echo "Skipping extractplayers.py for selective SEL run"; \
	else \
		python src/extractplayers.py; \
	fi
	@if [ -n "$(SEL)" ]; then \
		SELECT_DATA=$(SEL) python src/extractmatches.py; \
	else \
		python src/extractmatches.py; \
	fi

pipeline:
	@if [ -z "$(SEL)" ]; then echo "Provide SEL, e.g. make pipeline SEL=WBBL"; exit 1; fi
	python src/downloadpleyrs.py --select-data $(SEL)
	SELECT_DATA=$(SEL) python src/extractmatches.py


transform:
	cd dbtcrick && dbt build --profiles-dir .

# Nuke the staged data and DuckDB file to start fresh
clean:
# 	rm -rf data/rawdata/playerjsons/*.json
	rm -rf data/stageddata/*.parquet
	rm -f data/stageddata/crick.duckdb