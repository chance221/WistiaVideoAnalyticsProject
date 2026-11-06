from datetime import datetime, timezone
import json
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
import pytest
from scripts.aws.s3_client import (
    build_s3_bronze_key,
    upload_quarantine_json_to_s3,
    upload_raw_json_to_s3,
)


def test_build_s3_bronze_key_hive_partition_format():
  ts = datetime(2026, 11, 6, 17, 1, 25, tzinfo=timezone.utc)
  key = build_s3_bronze_key("media_events", "9k4tbcdfg0", ts)
  assert (
      key
      == "bronze/media_events/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  )


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
      ingested_at=ts,
  )

  assert (
      uri
      == "s3://wistia-pipeline-project/bronze/media_events/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  )
  mock_s3.put_object.assert_called_once()
  call_kwargs = mock_s3.put_object.call_args.kwargs
  assert call_kwargs["Bucket"] == "wistia-pipeline-project"
  assert json.loads(call_kwargs["Body"].decode("utf-8")) == raw_payload
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
    )


@patch("scripts.aws.s3_client.boto3.session.Session")
def test_upload_quarantine_json_to_s3_happy_and_sad_path(
    mock_session_cls: MagicMock,
):
  mock_s3 = MagicMock()
  mock_session_cls.return_value.client.return_value = mock_s3

  ts = datetime(2026, 11, 6, 17, 1, 25, tzinfo=timezone.utc)
  quarantine_items = [{"endpoint": "media_views", "raw_record": {"bad": 1}, "errors": []}]

  uri = upload_quarantine_json_to_s3(
      bucket_name="wistia-pipeline-project",
      region="us-east-1",
      endpoint_name="media_views",
      media_id="9k4tbcdfg0",
      quarantine_records=quarantine_items,
      quarantined_at=ts,
  )

  assert (
      uri
      == "s3://wistia-pipeline-project/quarantine/media_views/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  )
  call_kwargs = mock_s3.put_object.call_args.kwargs
  assert call_kwargs["Metadata"]["quarantine_count"] == "1"

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
        quarantined_at=ts,
    )

