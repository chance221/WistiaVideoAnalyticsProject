from unittest.mock import MagicMock, patch
import pytest
import requests
from scripts.raw.raw_ingest import lambda_handler, run_raw_ingestion


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
      lambda bucket_name, endpoint_name, media_id, **kwargs: (
          f"s3://{bucket_name}/bronze/{endpoint_name}/year=2026/month=11/day=06/{media_id}.json"
      )
  )

  response = lambda_handler({"env": "dev"}, None)

  assert response["statusCode"] == 200
  assert response["env"] == "dev"
  # 4 endpoints x 2 media_ids = 8 files uploaded
  assert response["files_uploaded"] == 8
  assert len(response["results"]) == 8
  assert mock_fetch.call_count == 8
  assert mock_upload.call_count == 8


@patch("scripts.raw.raw_ingest.fetch_paginated_data")
@patch("scripts.raw.raw_ingest.get_secret")
def test_run_raw_ingestion_sad_path_api_failure(
    mock_get_secret: MagicMock,
    mock_fetch: MagicMock,
):
  mock_get_secret.return_value = "mock-wistia-token"
  mock_fetch.side_effect = requests.exceptions.HTTPError("500 Server Error")

  with pytest.raises(requests.exceptions.HTTPError, match="500 Server Error"):
    run_raw_ingestion(env="dev")


def test_lambda_handler_sad_path_invalid_env():
  with pytest.raises(FileNotFoundError, match="no config for config 'invalid_env'"):
    lambda_handler({"env": "invalid_env"}, None)

