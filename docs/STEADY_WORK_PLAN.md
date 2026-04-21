# Steady work plan (next several weeks)

Purpose: keep **GitHub activity honest**—each push should reflect **real** progress (tests, docs, small fixes, operator UX). This is not a checklist for empty commits, backdated timestamps, or “touch a file” noise.

## Principles

1. **One coherent change per commit** (or a tight pair, e.g. test + fix). Prefer clear messages: what changed and why.
2. **Ship value you would defend in review**: runnable tests, clearer docs, safer defaults, measurable behavior.
3. **If you skip days**, resume on the next item—do not rewrite history or fake dates.
4. **Never commit** generated `data/`, secrets, or large binaries. Follow `.gitignore` and CI.

## How to use this doc

- Work top to bottom, or **swap days** when blocked (e.g. Docker down → do tests/docs only).
- After each item, tick it in your own notes or a short PR description.
- Optional rhythm: **0–1 merges per day** is fine; some days will be bigger, some none.

---

## Week 1 — Foundation and safety net

| Day | Area | Deliverable (pick one per day or combine if tiny) |
| --- | ---- | --------------------------------------------------- |
| 1 | Docs | Add or refine one section in `docs/LOCAL_RUNBOOK.md` (Windows/WSL note, port conflict, or `docker compose` verify step). |
| 2 | Tests | Add or extend a test in `tests/test_paths.py` or `tests/test_quality_rules_yaml.py` for an edge case you can name. |
| 3 | CI / quality | Open `.github/workflows/ci.yml` and ensure failure modes are clear; or run `ruff`/`pytest` locally and fix one real issue. |
| 4 | Ingestion | Small clarity pass on `ingestion/data_quality.py` or `ingestion/metrics_line.py` (docstring, log field, error message). |
| 5 | Streaming | One focused improvement in `streaming/bronze_stream.py` or `streaming/silver_stream.py` (logging, constant name, comment on checkpoint path). |

## Week 2 — Contracts and operator experience

| Day | Area | Deliverable |
| --- | ---- | ----------- |
| 6 | Schemas | Tighten `ingestion/schemas.py` or extend `tests/test_schema_contract.py` for one contract you rely on in Silver/Gold. |
| 7 | Silver / Gold | Document or code-comment one assumption in `streaming/silver_stream.py` or `streaming/gold_aggregations.py` (watermark, window, column). |
| 8 | Scripts | Improve `scripts/spark_submit_*.sh` or `Makefile` help text so a newcomer knows order: bronze → silver → gold. |
| 9 | Analytics CLI | UX or correctness tweak in `analytics/gold_query.py` (argparse help, exit code, empty dataset message). |
| 10 | Dashboard | Small Streamlit improvement in `analytics/dashboard.py` (empty state, chart label, cache key comment). |

## Week 3 — Resilience and demos

| Day | Area | Deliverable |
| --- | ---- | ----------- |
| 11 | Recovery | Expand `docs/RECOVERY.md` with one concrete scenario (checkpoint loss, topic reset, replay). |
| 12 | Demo | Add expected sample output or timing note to `docs/DEMO_SCRIPT.md`. |
| 13 | Producer | Hardening or observability in `ingestion/producer.py` / `ingestion/event_generator.py` (backoff, batch size note, metric line). |
| 14 | DQ rules | One justified change in `ingestion/quality_rules.yaml` **plus** test coverage in `tests/test_data_quality.py` or related. |
| 15 | Docker | Clarify `docker-compose.yml` or `Dockerfile.spark` comment; or document resource limits in runbook. |

## Week 4 — Depth and release hygiene

| Day | Area | Deliverable |
| --- | ---- | ----------- |
| 16 | Tests | Add coverage for `ingestion/metrics_line.py` or streaming metric NDJSON shape (`tests/test_metrics_line.py`). |
| 17 | Packaging | Review `requirements.txt`: pin or loosen with a sentence in commit body on **why** (repro vs. security patch). |
| 18 | Security / hygiene | Repo scan for secrets; tighten `.gitignore` if needed; optional `pre-commit` only if team will use it. |
| 19 | Performance / ops | Short doc on Spark tuning knobs or batch sizing; tie to one code comment where relevant. |
| 20 | Polish | README diagram or badge text only if it reduces confusion; link any new doc sections. |

---

## Rolling backlog (after day 20)

Use when the table above is done or you want variety:

- **Regression tests** for quarantine path and out-of-range events.
- **Issue-driven** fixes from your own runs (compose version, Spark driver memory).
- **Version tag** when a milestone is real: summarize changes and how to verify (`make test`, demo script).

## Anti-patterns (do not do these for “green squares”)

- Whitespace-only or comment-only commits with no reader benefit.
- Copy-paste duplicate files, lorem ipsum docs, or disabled CI to force green.
- Splitting one logical change into many commits the same hour without review value.

When in doubt, ask: **Would I merge this if someone else opened the PR?** If yes, ship it.
