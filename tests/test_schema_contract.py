"""Contract tests: producer output matches the canonical Bronze schema,
and quality_rules.yaml only references fields the schema contains.

The canonical schema is defined here as the *contract* and must stay in sync
with streaming/silver_stream.py:bronze_schema.  If either side drifts, one of
these tests will fail.
"""
from __future__ import annotations

import os

from ingestion.event_generator import generate_event
from ingestion.data_quality import load_quality_rules

# Canonical field contract -- mirrors streaming/silver_stream.py bronze_schema.
SCHEMA_TOP_KEYS = {"event_id", "timestamp", "event_type", "source", "status", "metrics", "extra"}
SCHEMA_METRICS_KEYS = {"distance", "temperature", "amount", "duration"}
SCHEMA_EXTRA_KEYS = {"note"}
SCHEMA_ALL_LEAF_FIELDS = (
    (SCHEMA_TOP_KEYS - {"metrics", "extra"}) | SCHEMA_METRICS_KEYS | SCHEMA_EXTRA_KEYS
)


# -----------------------------------------------------------------------
# 1. Producer payload matches the canonical Bronze schema contract
# -----------------------------------------------------------------------

def test_producer_top_level_keys():
    event = generate_event()
    assert set(event.keys()) == SCHEMA_TOP_KEYS


def test_producer_metrics_keys():
    event = generate_event()
    assert set(event["metrics"].keys()) == SCHEMA_METRICS_KEYS


def test_producer_extra_keys():
    event = generate_event()
    assert set(event["extra"].keys()) == SCHEMA_EXTRA_KEYS


# -----------------------------------------------------------------------
# 2. Quality rules only reference leaf fields in the schema
# -----------------------------------------------------------------------

def test_quality_rules_fields_exist_in_schema():
    rules_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "ingestion",
        "quality_rules.yaml",
    )
    rules = load_quality_rules(rules_path)

    for field in rules.get("numeric_ranges", {}):
        assert field in SCHEMA_ALL_LEAF_FIELDS, (
            f"quality_rules.yaml references '{field}' which is not a leaf field in the schema contract"
        )
