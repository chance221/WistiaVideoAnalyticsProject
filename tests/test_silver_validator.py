from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest
from scripts.silver.validator import (
    process_and_quarantine_invalid_records,
    validate_bronze_records,
)


def test_validate_bronze_records_happy_path():
  ts = datetime(2026, 11, 6, 18, 0, 0, tzinfo=timezone.utc)
  payload = {
      "id": 128512814,
      "hashed_id": "9k4tbcdfg0",
      "name": "The Gap Method",
      "stats": {
          "pageLoads": 940,
          "visitors": 706,
          "percentOfVisitorsClickingPlay": 42,
          "plays": 328,
          "averagePercentWatched": 26,
      },
  }

  valid, quarantine = validate_bronze_records("media_views", payload, validated_at=ts)
  assert len(valid) == 1
  assert len(quarantine) == 0
  assert valid[0]["hashed_id"] == "9k4tbcdfg0"
  assert valid[0]["ingested_at"] == "2026-11-06T18:00:00+00:00"


def test_validate_bronze_records_sad_path_mixed_and_invalid_inputs():
  ts = datetime(2026, 11, 6, 18, 0, 0, tzinfo=timezone.utc)
  mixed_payload = [
      {"engagement": 0.45, "engagement_data": [10, 9, 8]},
      {"engagement": "invalid-float", "engagement_data": "not-a-list"},
      "non-dict-item",
  ]

  valid, quarantine = validate_bronze_records(
      "media_engagement", mixed_payload, validated_at=ts
  )
  assert len(valid) == 1
  assert len(quarantine) == 2
  assert quarantine[0]["raw_record"]["engagement"] == "invalid-float"
  assert quarantine[1]["raw_record"] == "non-dict-item"

  with pytest.raises(ValueError, match="Unknown endpoint 'unknown_ep'"):
    validate_bronze_records("unknown_ep", {})

  with pytest.raises(TypeError, match="Expected dict or list"):
    validate_bronze_records("media_views", "invalid-string-payload")  # type: ignore[arg-type]


@patch("scripts.silver.validator.upload_quarantine_json_to_s3")
def test_process_and_quarantine_invalid_records_happy_and_sad_path(
    mock_upload_quarantine: MagicMock,
):
  mock_upload_quarantine.return_value = (
      "s3://wistia-pipeline-project/quarantine/media_engagement/year=2026/month=11/day=06/9k4tbcdfg0.json"
  )

  # Happy path: all valid, no quarantine upload triggered
  clean_res = process_and_quarantine_invalid_records(
      bucket_name="wistia-pipeline-project",
      region="us-east-1",
      endpoint_name="media_engagement",
      media_id="9k4tbcdfg0",
      raw_payload={"engagement": 0.5, "engagement_data": [1, 2, 3]},
  )
  assert len(clean_res["valid_records"]) == 1
  assert len(clean_res["quarantine_records"]) == 0
  assert clean_res["quarantine_s3_uri"] is None
  mock_upload_quarantine.assert_not_called()

  # Sad path: invalid record triggers quarantine S3 upload
  dirty_res = process_and_quarantine_invalid_records(
      bucket_name="wistia-pipeline-project",
      region="us-east-1",
      endpoint_name="media_engagement",
      media_id="9k4tbcdfg0",
      raw_payload={"engagement": "bad", "engagement_data": []},
  )
  assert len(dirty_res["valid_records"]) == 0
  assert len(dirty_res["quarantine_records"]) == 1
  assert dirty_res["quarantine_s3_uri"] is not None
  mock_upload_quarantine.assert_called_once()
