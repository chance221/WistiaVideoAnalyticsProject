from datetime import datetime, timezone
import json
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
import pytest
from scripts.aws.s3_client import (
    build_s3_bronze_key,
    list_bronze_s3_keys,
    parse_media_id_from_s3_key,
    read_json_from_s3,
    upload_quarantine_json_to_s3,
    upload_raw_json_to_s3,
)


def test_build_s3_bronze_key_hive_partition_format_happy_and_sad_path():
  ts = datetime(2026, 11, 6, 17, 1, 25, tzinfo=timezone.utc)
  dev_key = build_s3_bronze_key("media_events", "9k4tbcdfg0", ts, env="dev")
  assert (
      dev_key
      == "dev/bronze/media_events/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  )

  prod_key = build_s3_bronze_key("media_events", "9k4tbcdfg0", ts, env="prod")
  assert (
      prod_key
      == "prod/bronze/media_events/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  )

  with pytest.raises(ValueError, match="env must be a non-empty string"):
    build_s3_bronze_key("media_events", "9k4tbcdfg0", ts, env="   ")


@patch("scripts.aws.s3_client.boto3.session.Session")
def test_upload_raw_json_to_s3_preserves_unaltered_payload(
    mock_session_cls: MagicMock,
):
  mock_s3 = MagicMock()
  mock_session_cls.return_value.client.return_value = mock_s3

  ts = datetime(2026, 11, 6, 17, 1, 25, tzinfo=timezone.utc)
  raw_payload = [{"event_key": "abc", "percent_viewed": 0.75}]

  uri = upload_raw_json_to_s3(
      bucket_name="wistia-pipeline-project",
      region="us-east-1",
      endpoint_name="media_events",
      media_id="9k4tbcdfg0",
      data=raw_payload,
      env="dev",
      ingested_at=ts,
  )

  assert (
      uri
      == "s3://wistia-pipeline-project/dev/bronze/media_events/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  )
  mock_s3.put_object.assert_called_once()
  call_kwargs = mock_s3.put_object.call_args.kwargs
  assert call_kwargs["Bucket"] == "wistia-pipeline-project"
  assert json.loads(call_kwargs["Body"].decode("utf-8")) == raw_payload
  assert call_kwargs["Metadata"]["env"] == "dev"
  assert call_kwargs["Metadata"]["endpoint"] == "media_events"
  assert call_kwargs["Metadata"]["media_id"] == "9k4tbcdfg0"


@patch("scripts.aws.s3_client.boto3.session.Session")
def test_upload_raw_json_to_s3_raises_runtime_error_on_client_error(
    mock_session_cls: MagicMock,
):
  mock_s3 = MagicMock()
  mock_s3.put_object.side_effect = ClientError(
      {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
      "PutObject",
  )
  mock_session_cls.return_value.client.return_value = mock_s3

  with pytest.raises(RuntimeError, match="Failed to upload raw JSON"):
    upload_raw_json_to_s3(
        bucket_name="wistia-pipeline-project",
        region="us-east-1",
        endpoint_name="media_views",
        media_id="9k4tbcdfg0",
        data={"id": 1},
        env="prod",
    )


@patch("scripts.aws.s3_client.boto3.session.Session")
def test_upload_quarantine_json_to_s3_happy_and_sad_path(
    mock_session_cls: MagicMock,
):
  mock_s3 = MagicMock()
  mock_session_cls.return_value.client.return_value = mock_s3

  ts = datetime(2026, 11, 6, 17, 1, 25, tzinfo=timezone.utc)
  quarantine_items = [{"layer": "silver", "endpoint": "media_views", "raw_record": {"bad": 1}, "errors": []}]

  silver_uri = upload_quarantine_json_to_s3(
      bucket_name="wistia-pipeline-project",
      region="us-east-1",
      endpoint_name="media_views",
      media_id="9k4tbcdfg0",
      quarantine_records=quarantine_items,
      env="prod",
      stage="silver",
      quarantined_at=ts,
  )

  assert (
      silver_uri
      == "s3://wistia-pipeline-project/prod/quarantine/silver/media_views/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  )
  call_kwargs = mock_s3.put_object.call_args.kwargs
  assert call_kwargs["Metadata"]["quarantine_count"] == "1"
  assert call_kwargs["Metadata"]["env"] == "prod"
  assert call_kwargs["Metadata"]["stage"] == "silver"

  gold_uri = upload_quarantine_json_to_s3(
      bucket_name="wistia-pipeline-project",
      region="us-east-1",
      endpoint_name="dim_media",
      media_id="batch",
      quarantine_records=quarantine_items,
      env="dev",
      stage="gold",
      quarantined_at=ts,
  )
  assert (
      gold_uri
      == "s3://wistia-pipeline-project/dev/quarantine/gold/dim_media/year=2026/month=11/day=06/batch_20261106T170125Z.json"
  )
  assert mock_s3.put_object.call_args.kwargs["Metadata"]["stage"] == "gold"

  with pytest.raises(ValueError, match="Invalid quarantine stage"):
    upload_quarantine_json_to_s3(
        bucket_name="wistia-pipeline-project",
        region="us-east-1",
        endpoint_name="media_views",
        media_id="9k4tbcdfg0",
        quarantine_records=quarantine_items,
        env="prod",
        stage="bronze",
        quarantined_at=ts,
    )

  mock_s3.put_object.side_effect = ClientError(
      {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
      "PutObject",
  )
  with pytest.raises(RuntimeError, match="Failed to upload quarantine JSON"):
    upload_quarantine_json_to_s3(
        bucket_name="wistia-pipeline-project",
        region="us-east-1",
        endpoint_name="media_views",
        media_id="9k4tbcdfg0",
        quarantine_records=quarantine_items,
        env="prod",
        stage="silver",
        quarantined_at=ts,
    )


def test_parse_media_id_from_s3_key_happy_and_sad_path():
  key = "dev/bronze/media_events/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  assert parse_media_id_from_s3_key(key) == "9k4tbcdfg0"

  with pytest.raises(ValueError, match="Invalid Bronze S3 key filename format"):
    parse_media_id_from_s3_key("dev/bronze/media_events/invalid_filename.txt")

  with pytest.raises(ValueError, match="Empty media_id"):
    parse_media_id_from_s3_key("dev/bronze/media_events/_20261106T170125Z.json")


@patch("scripts.aws.s3_client.boto3.session.Session")
def test_list_bronze_s3_keys_happy_and_sad_path(mock_session_cls: MagicMock):
  mock_s3 = MagicMock()
  mock_paginator = MagicMock()
  mock_paginator.paginate.return_value = [
      {
          "Contents": [
              {"Key": "dev/bronze/media_views/year=2026/month=11/day=06/8hunphufxp_20261106T170125Z.json"},
              {"Key": "dev/bronze/media_views/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"},
              {"Key": "dev/bronze/media_views/year=2026/month=11/day=06/_SUCCESS"},
          ]
      }
  ]
  mock_s3.get_paginator.return_value = mock_paginator
  mock_session_cls.return_value.client.return_value = mock_s3

  ts = datetime(2026, 11, 6, 12, 0, 0, tzinfo=timezone.utc)
  keys = list_bronze_s3_keys(
      "wistia-pipeline-project",
      "us-east-1",
      "media_views",
      env="dev",
      partition_date=ts,
  )
  assert len(keys) == 2
  assert keys[0].startswith("dev/bronze/")

  with pytest.raises(ValueError, match="env must be a non-empty string"):
    list_bronze_s3_keys("wistia-pipeline-project", "us-east-1", "media_views", env="")

  mock_paginator.paginate.side_effect = ClientError(
      {"Error": {"Code": "NoSuchBucket", "Message": "Bucket missing"}},
      "ListObjectsV2",
  )
  with pytest.raises(RuntimeError, match="Failed to list Bronze keys"):
    list_bronze_s3_keys("wistia-pipeline-project", "us-east-1", "media_views", env="dev")


@patch("scripts.aws.s3_client.boto3.session.Session")
def test_read_json_from_s3_happy_and_sad_path(mock_session_cls: MagicMock):
  mock_s3 = MagicMock()
  mock_body = MagicMock()
  mock_body.read.return_value = b'{"hashed_id": "9k4tbcdfg0", "id": 123}'
  mock_s3.get_object.return_value = {"Body": mock_body}
  mock_session_cls.return_value.client.return_value = mock_s3

  data = read_json_from_s3("wistia-pipeline-project", "us-east-1", "dev/bronze/test.json")
  assert data["hashed_id"] == "9k4tbcdfg0"

  # Sad path: malformed JSON in S3 object
  mock_body.read.return_value = b"not-valid-json{"
  with pytest.raises(RuntimeError, match="Failed to read JSON"):
    read_json_from_s3("wistia-pipeline-project", "us-east-1", "dev/bronze/test.json")
