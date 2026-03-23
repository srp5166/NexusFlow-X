# Restart and recovery (local stack)

This document describes **checkpoint resumption**, **Kafka behavior**, and **safe reset** for development. For commands to start jobs, see [LOCAL_RUNBOOK.md](LOCAL_RUNBOOK.md).

## What persists on disk

| Path (host) | Purpose |
|-------------|---------|
| `data/checkpoints/bronze` | Spark Structured Streaming checkpoint for Bronze (Kafka offsets + state) |
| `data/checkpoints/silver` | Checkpoint for Silver (file stream from Bronze Parquet) |
| `data/checkpoints/gold_hourly` | Checkpoint for Gold (Silver Parquet → aggregates) |
| `data/bronze`, `data/silver`, `data/gold/...` | Output Parquet |
| `data/quarantine/...` | Out-of-range rows (append) |
| `data/metrics/pipeline_metrics.jsonl` | Per-batch NDJSON metrics (append) |

Bind mount: repo `data/` ↔ `/app/data` in `nexus-spark`.

## Spark container restart

- **Bronze:** On restart, if you run the same `spark-submit` with the **same checkpoint directory**, Structured Streaming **resumes** from the last committed offset (Kafka consumer group state in the checkpoint). New events are processed from Kafka according to that state.
- **Silver / Gold:** File-stream checkpoints remember which files were processed. Resuming usually **avoids reprocessing** already committed files; behavior is defined by Spark’s file source + your checkpoint version.

If you **delete** a layer’s checkpoint directory while keeping Parquet data, the next run may **re-read** files (depending on `maxFilesPerTrigger`, `maxFileAge`, and whether the query is treated as new). Treat checkpoint deletion as **resetting** that layer’s consumer state.

## Kafka restart

- While Kafka is down, streaming queries **fail** or stall; fix Kafka first, then restart the Spark job.
- **Bronze** uses `startingOffsets` = **latest** in [streaming/bronze_stream.py](../streaming/bronze_stream.py) only for **new** queries without a checkpoint. With an **existing checkpoint**, offsets come from the checkpoint, not `latest`.

## Development reset (replay from scratch)

**Warning:** Deletes pipeline state and/or outputs. Use only on disposable data.

1. Stop streaming jobs (Ctrl+C in the terminal running `spark-submit`, or stop the container).
2. Optional: `docker compose down`.
3. Remove state you want to rebuild, e.g.:
   - Full reset: delete contents under `data/bronze`, `data/silver`, `data/gold`, `data/checkpoints`, `data/quarantine`, `data/metrics` (or delete entire `data/` tree if you do not need it).
4. `docker compose up -d`, recreate topic if needed, then run Bronze → producer → Silver → Gold per the runbook.

## What this doc does not guarantee

- No formal SLA for recovery time.
- Multi-broker Kafka and production replication are out of scope for the default compose file.
- Exact duplicate semantics after partial deletes require inspecting Spark checkpoint metadata and logs.

See also [validation-log.md](validation-log.md) — **What is guaranteed vs not guaranteed**.
