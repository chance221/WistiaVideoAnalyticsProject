from unittest.mock import MagicMock, patch
import pytest
from scripts.silver.bronze_to_silver import (
    build_iceberg_scd1_merge_sql,
    process_bronze_endpoint_to_silver,
    run_silver_pipeline,
    select_latest_s3_keys_per_media,
    upsert_silver_iceberg_table,
)


def test_select_latest_s3_keys_per_media_happy_and_sad_path():
  keys = [
      "dev/bronze/media_views/year=2026/month=11/day=05/9k4tbcdfg0_20261105T100000Z.json",
      "dev/bronze/media_views/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json",
      "dev/bronze/media_views/year=2026/month=11/day=05/8hunphufxp_20261105T100000Z.json",
      "dev/bronze/media_views/year=2026/month=11/day=06/8hunphufxp_20261106T170125Z.json",
  ]
  selected = select_latest_s3_keys_per_media(keys)
  assert selected == [
      "dev/bronze/media_views/year=2026/month=11/day=06/8hunphufxp_20261106T170125Z.json",
      "dev/bronze/media_views/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json",
  ]

  with pytest.raises(TypeError, match="s3_keys must be a list"):
    select_latest_s3_keys_per_media("not-a-list")  # type: ignore[arg-type]

  with pytest.raises(ValueError, match="Invalid Bronze S3 key filename format"):
    select_latest_s3_keys_per_media(["dev/bronze/media_views/bad_file.txt"])


def test_build_iceberg_scd1_merge_sql_happy_and_sad_path():
  sql = build_iceberg_scd1_merge_sql(
      catalog_name="glue_catalog",
      database_name="wistia_silver_dev",
      table_name="media_events",
      source_view_name="source_media_events",
      primary_key="event_key",
  )
  assert "MERGE INTO glue_catalog.wistia_silver_dev.media_events AS target" in sql
  assert "ON target.event_key = source.event_key" in sql
  assert "WHEN MATCHED THEN UPDATE SET *" in sql
  assert "WHEN NOT MATCHED THEN INSERT *" in sql

  with pytest.raises(ValueError, match="must all be non-empty"):
    build_iceberg_scd1_merge_sql("glue_catalog", "", "media_events", "src", "event_key")


def test_upsert_silver_iceberg_table_create_and_merge_happy_and_sad_path():
  mock_spark = MagicMock()
  records = [{"hashed_id": "9k4tbcdfg0", "plays": 328, "ingested_at": "2026-11-06T17:00:00Z"}]

  # Table does not exist yet -> creates new Iceberg v2 table under <env>/silver/
  mock_spark.catalog.tableExists.return_value = False
  count = upsert_silver_iceberg_table(
      spark=mock_spark,
      records=records,
      endpoint_name="media_views",
      bucket_name="wistia-pipeline-project",
      env="dev",
  )
  assert count == 1
  mock_spark.createDataFrame.return_value.writeTo.assert_called_once_with(
      "glue_catalog.wistia_silver_dev.media_views"
  )

  # Table exists -> executes SCD1 MERGE INTO SQL
  mock_spark.reset_mock()
  mock_spark.catalog.tableExists.return_value = True
  count_merge = upsert_silver_iceberg_table(
      spark=mock_spark,
      records=records,
      endpoint_name="media_views",
      bucket_name="wistia-pipeline-project",
      env="prod",
  )
  assert count_merge == 1
  sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
  assert any("MERGE INTO glue_catalog.wistia_silver_prod.media_views" in s for s in sql_calls)

  # Empty records -> no-op returns 0
  assert upsert_silver_iceberg_table(mock_spark, [], "media_views", "wistia-pipeline-project") == 0

  # Sad path: invalid endpoint
  with pytest.raises(ValueError, match="Unsupported endpoint"):
    upsert_silver_iceberg_table(mock_spark, records, "invalid_ep", "wistia-pipeline-project")


@patch("scripts.silver.bronze_to_silver.read_json_from_s3")
@patch("scripts.silver.bronze_to_silver.list_bronze_s3_keys")
def test_process_bronze_endpoint_to_silver_happy_and_sad_path(
    mock_list_keys: MagicMock,
    mock_read_json: MagicMock,
):
  mock_list_keys.return_value = [
      "dev/bronze/media_engagement/year=2026/month=11/day=06/9k4tbcdfg0_20261106T170125Z.json"
  ]
  mock_read_json.return_value = {
      "engagement": 0.255,
      "engagement_data": [100, 90, 80],
      "rewatch_data": [5, 4, 3],
  }
  mock_spark = MagicMock()
  mock_spark.catalog.tableExists.return_value = True

  summary = process_bronze_endpoint_to_silver(
      bucket_name="wistia-pipeline-project",
      region="us-east-1",
      endpoint_name="media_engagement",
      env="dev",
      spark=mock_spark,
  )

  assert summary["files_processed"] == 1
  assert summary["silver_record_count"] == 1
  assert summary["quarantine_record_count"] == 0
  assert summary["iceberg_upserted_count"] == 1
  assert summary["silver_records"][0]["media_id"] == "9k4tbcdfg0"

  with pytest.raises(ValueError, match="Unsupported endpoint"):
    process_bronze_endpoint_to_silver("wistia-pipeline-project", "us-east-1", "bad_ep")


@patch("scripts.silver.bronze_to_silver.process_bronze_endpoint_to_silver")
def test_run_silver_pipeline_happy_and_sad_path(mock_process_ep: MagicMock):
  mock_process_ep.side_effect = lambda endpoint_name, **kwargs: {
      "endpoint": endpoint_name,
      "silver_record_count": 2,
  }

  results = run_silver_pipeline(env="dev")
  assert len(results) == 4
  assert mock_process_ep.call_count == 4

  with pytest.raises(FileNotFoundError):
    run_silver_pipeline(env="nonexistent_env")
