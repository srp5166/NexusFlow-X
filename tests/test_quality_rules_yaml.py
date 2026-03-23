import os

import yaml


def test_quality_rules_loads_and_has_numeric_ranges():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, "ingestion", "quality_rules.yaml")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert "numeric_ranges" in data
    for key in ("distance", "temperature", "amount", "duration"):
        assert key in data["numeric_ranges"]
        assert "min" in data["numeric_ranges"][key]
        assert "max" in data["numeric_ranges"][key]
