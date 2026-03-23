# NexusFlow-X — operator shortcuts (run from repo root; use WSL/Git Bash on Windows).
# Requires Docker Compose and running containers for exec targets.

.PHONY: up down bronze silver gold producer test validate query dashboard

up:
	docker compose up -d

down:
	docker compose down

bronze:
	docker exec nexus-spark bash -c "cd /app && export PYTHONPATH=/app && bash scripts/spark_submit_bronze.sh"

silver:
	docker exec nexus-spark bash -c "cd /app && export PYTHONPATH=/app && bash scripts/spark_submit_silver.sh"

gold:
	docker exec nexus-spark bash -c "cd /app && export PYTHONPATH=/app && bash scripts/spark_submit_gold.sh"

producer:
	docker exec nexus-spark bash -c "cd /app && export PYTHONPATH=/app && python3 -m ingestion.producer"

test:
	python -m pytest tests/ -q

validate:
	python -c "import yaml; yaml.safe_load(open('ingestion/quality_rules.yaml')); print('quality_rules.yaml OK')"

query:
	python analytics/gold_query.py

dashboard:
	streamlit run analytics/dashboard.py
