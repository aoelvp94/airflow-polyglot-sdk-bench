# Local demo: Airflow 3.4.0 + Go/Java/TS task handlers + dbt via Cosmos.
COMPOSE ?= docker compose
PSQL     = $(COMPOSE) exec -T warehouse psql -U wh -d warehouse -v ON_ERROR_STOP=1
REPEATS ?= 3
LANGS   ?= python,go,java,ts
TABLES  ?= orders_1m,orders_10m
AIRFLOW_PORT ?= 18080
# Not LANG: that is the shell's locale variable and is almost always already set.
INGEST_LANG ?= $(or $(INGEST_LANGUAGE),auto)
TABLE   ?= orders_1m

.PHONY: build up seed bench report run down clean logs ps

build:            ## Build the Airflow image (Go/Java/TS bundles + dbt manifest)
	$(COMPOSE) build

up: build         ## Start Postgres x2 + Airflow (api-server :AIRFLOW_PORT, scheduler, dag-processor)
	AIRFLOW_PORT=$(AIRFLOW_PORT) $(COMPOSE) up -d --wait
	@echo "Airflow UI: http://localhost:$(AIRFLOW_PORT)"

seed:             ## Create src.orders_1m and src.orders_10m (deterministic; ~1 min for 10M)
	$(PSQL) -v rows=1000000  -v tbl=orders_1m  -f /sql/seed.sql
	$(PSQL) -v rows=10000000 -v tbl=orders_10m -f /sql/seed.sql

bench:            ## Run the ingestion benchmark: REPEATS x LANGS x TABLES, then summarize
	python3 bench/run_bench.py --repeats $(REPEATS) --langs $(LANGS) --tables $(TABLES) --out results/bench_raw.jsonl
	python3 bench/summarize.py results/bench_raw.jsonl > results/bench_summary.md
	@cat results/bench_summary.md

report:           ## Re-summarize existing raw benchmark results
	python3 bench/summarize.py results/bench_raw.jsonl > results/bench_summary.md
	@cat results/bench_summary.md

run:              ## Trigger orders_elt_pipeline once (INGEST_LANG=auto|go|java|ts|python TABLE=orders_1m|orders_10m) and wait
	python3 bench/run_pipeline.py --lang $(INGEST_LANG) --table $(TABLE)

down:             ## Stop containers (keeps volumes/data)
	$(COMPOSE) down

clean:            ## Stop containers and delete volumes
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100 airflow-scheduler

ps:
	$(COMPOSE) ps
