from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest
import requests
from scripts.raw.raw_ingest import (
    lambda_handler,
    resolve_ingestion_date_window,
    run_raw_ingestion,
)


def test_resolve_ingestion_date_window_happy_and_sad_paths():
  # 1. Default scheduled run (calculates yesterday from EventBridge `time` or reference_time)
  eb_event = {"time": "2026-11-06T06:00:00Z"}
  assert resolve_ingestion_date_window(eb_event) == ("2026-11-05", "2026-11-05")

  ref_dt = datetime(2026, 11, 6, 12, 0, 0, tzinfo=timezone.utc)
  assert resolve_ingestion_date_window({}, reference_time=ref_dt) == (
      "2026-11-05",
      "2026-11-05",
  )

  # 2. Explicit start_date and end_date in event or queryStringParameters
  assert resolve_ingestion_date_window(
      {"start_date": "2026-10-01", "end_date": "2026-10-31"}
  ) == ("2026-10-01", "2026-10-31")

  assert resolve_ingestion_date_window(
      {"queryStringParameters": {"start_date": "2026-11-01", "end_date": "2026-11-02"}}
  ) == ("2026-11-01", "2026-11-02")

  # 3. Full historical 2-year backfill flag
  assert resolve_ingestion_date_window({"full_load": True}) == (None, None)

  # 4. Sad paths: missing one bound, end_date < start_date, bad format, bad EventBridge time
  with pytest.raises(ValueError, match="must be provided together"):
    resolve_ingestion_date_window({"start_date": "2026-11-01"})

  with pytest.raises(ValueError, match="is before start date"):
    resolve_ingestion_date_window(
        {"start_date": "2026-11-05", "end_date": "2026-11-01"}
    )

  with pytest.raises(ValueError, match="Invalid EventBridge 'time'"):
    resolve_ingestion_date_window({"time": "not-a-timestamp"})


@patch("scripts.raw.raw_ingest.upload_raw_json_to_s3")
@patch("scripts.raw.raw_ingest.fetch_paginated_data")
@patch("scripts.raw.raw_ingest.get_secret")
def test_run_raw_ingestion_and_lambda_handler_happy_path(
    mock_get_secret: MagicMock,
    mock_fetch: MagicMock,
    mock_upload: MagicMock,
):
  mock_get_secret.return_value = "mock-wistia-token"
  mock_fetch.side_effect = lambda url, **kwargs: (
      [{"event_key": "e1"}, {"event_key": "e2"}]
      if "events" in url
      else {"id": 123}
  )
  mock_upload.side_effect = (
      lambda bucket_name, endpoint_name, media_id, env="dev", **kwargs: (
          f"s3://{bucket_name}/{env}/bronze/{endpoint_name}/year=2026/month=11/day=06/{media_id}.json"
      )
  )

  response = lambda_handler(
      {"env": "dev", "time": "2026-11-06T06:00:00Z"}, None
  )

  assert response["statusCode"] == 200
  assert response["env"] == "dev"
  assert response["start_date"] == "2026-11-05"
  assert response["end_date"] == "2026-11-05"
  # 4 endpoints x 2 media_ids = 8 files uploaded
  assert response["files_uploaded"] == 8
  assert len(response["results"]) == 8
  assert response["results"][0]["s3_uri"].startswith(
      "s3://wistia-pipeline-project/dev/bronze/"
  )
  assert mock_fetch.call_count == 8
  assert mock_upload.call_count == 8


@patch("scripts.raw.raw_ingest.fetch_paginated_data")
@patch("scripts.raw.raw_ingest.get_secret")
def test_run_raw_ingestion_sad_path_api_failure_and_bad_dates(
    mock_get_secret: MagicMock,
    mock_fetch: MagicMock,
):
  with pytest.raises(ValueError, match="must be provided together"):
    run_raw_ingestion(env="dev", start_date="2026-11-05", end_date=None)

  mock_get_secret.return_value = "mock-wistia-token"
  mock_fetch.side_effect = requests.exceptions.HTTPError("500 Server Error")

  with pytest.raises(requests.exceptions.HTTPError, match="500 Server Error"):
    run_raw_ingestion(env="dev", start_date="2026-11-05", end_date="2026-11-05")


def test_lambda_handler_sad_path_invalid_env():
  with pytest.raises(FileNotFoundError, match="no config for config 'invalid_env'"):
    lambda_handler({"env": "invalid_env"}, None)
