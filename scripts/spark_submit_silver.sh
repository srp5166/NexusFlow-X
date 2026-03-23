#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export HOME="${HOME:-/tmp}"
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)"
/opt/spark/bin/spark-submit \
  --master 'local[2]' \
  streaming/silver_stream.py
