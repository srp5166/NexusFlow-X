#!/usr/bin/env bash
# From WSL/Linux (Docker on host): submit Gold job in nexus-spark container.
set -euo pipefail
docker exec -it nexus-spark bash -c 'cd /app && export PYTHONPATH=/app && /opt/spark/bin/spark-submit --master local[2] --driver-memory 2g streaming/gold_aggregations.py'
