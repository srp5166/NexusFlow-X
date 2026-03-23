import json

from ingestion.metrics_line import append_pipeline_metric


def test_append_pipeline_metric_writes_ndjson(monkeypatch, tmp_path):
    monkeypatch.setenv("NEXUSFLOW_DATA_ROOT", str(tmp_path))
    append_pipeline_metric("bronze", 1, 42, extra={"k": "v"})
    p = tmp_path / "metrics" / "pipeline_metrics.jsonl"
    assert p.is_file()
    line = p.read_text(encoding="utf-8").strip()
    obj = json.loads(line)
    assert obj["layer"] == "bronze"
    assert obj["batch_id"] == 1
    assert obj["row_count"] == 42
    assert obj["extra"]["k"] == "v"
