#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export HOME="${HOME:-/tmp}"
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)"
/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  streaming/bronze_stream.py
