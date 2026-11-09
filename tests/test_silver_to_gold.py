from datetime import timezone
from unittest.mock import MagicMock, patch
import pytest
from scripts.aws.glue_spark import (
    build_iceberg_spark_configs,
    parse_partition_date_arg,
    run_glue_medallion_job,
)
from scripts.gold.silver_to_gold import (
    read_silver_table_records,
    run_gold_pipeline,
    upsert_gold_iceberg_table,
)


def test_upsert_gold_iceberg_table_happy_and_sad_path():
  mock_spark = MagicMock()
  rows = [{"media_id": "9k4tbcdfg0", "title": "The Gap Method"}]

  # Table does not exist -> creates Iceberg v2 table
  mock_spark.catalog.tableExists.return_value = False
  count = upsert_gold_iceberg_table(
      spark=mock_spark,
      records=rows,
      table_name="dim_media",
      bucket_name="wistia-pipeline-project",
      env="dev",
  )
  assert count == 1
  mock_spark.createDataFrame.return_value.writeTo.assert_called_once_with(
      "glue_catalog.wistia_gold_dev.dim_media"
  )

  # Table exists -> executes MERGE INTO
  mock_spark.reset_mock()
  mock_spark.catalog.tableExists.return_value = True
  count_merge = upsert_gold_iceberg_table(
      spark=mock_spark,
      records=rows,
      table_name="dim_media",
      bucket_name="wistia-pipeline-project",
      env="prod",
  )
  assert count_merge == 1
  sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
  assert any("MERGE INTO glue_catalog.wistia_gold_prod.dim_media" in s for s in sql_calls)

  # Empty records -> 0
  assert upsert_gold_iceberg_table(mock_spark, [], "dim_media", "wistia-pipeline-project") == 0

  # Sad path: invalid table name
  with pytest.raises(ValueError, match="Unsupported Gold table"):
    upsert_gold_iceberg_table(mock_spark, rows, "bad_table", "wistia-pipeline-project")


def test_read_silver_table_records_happy_and_sad_path():
  mock_spark = MagicMock()
  mock_row = MagicMock()
  mock_row.asDict.return_value = {"hashed_id": "9k4tbcdfg0"}
  mock_spark.catalog.tableExists.return_value = True
  mock_spark.table.return_value.collect.return_value = [mock_row]

  records = read_silver_table_records(mock_spark, "media_views", env="dev")
  assert records == [{"hashed_id": "9k4tbcdfg0"}]

  mock_spark.catalog.tableExists.return_value = False
  assert read_silver_table_records(mock_spark, "media_views", env="dev") == []

  with pytest.raises(ValueError, match="Unsupported Silver table"):
    read_silver_table_records(mock_spark, "bad_silver_table")


def test_run_gold_pipeline_happy_and_sad_path():
  mock_spark = MagicMock()
  mock_spark.catalog.tableExists.return_value = True

  silver_data = {
      "media_views": [{
          "hashed_id": "9k4tbcdfg0",
          "page_loads": 100,
          "visitors": 80,
          "plays": 40,
          "percent_of_visitors_clicking_play": 50,
          "average_percent_watched": 60,
      }],
      "media_engagement": [{
          "media_id": "9k4tbcdfg0",
          "engagement": 0.5,
          "engagement_data": [10, 20],
      }],
      "media_metadata": [{
          "id": 1,
          "hashed_id": "9k4tbcdfg0",
          "name": "Video 1",
          "duration": 100.0,
          "type": "Video",
          "status": "ready",
          "archived": False,
          "thumbnail_url": "https://wistia.com/t.jpg",
          "created": "2025-01-01T00:00:00Z",
          "updated": "2026-01-01T00:00:00Z",
          "ingested_at": "2026-11-06T17:00:00Z",
      }],
      "media_events": [{
          "event_key": "e1",
          "media_id": "9k4tbcdfg0",
          "media_url": "https://wistia.com/medias/9k4tbcdfg0",
          "embed_url": "https://dataengineerinterviews.com/",
          "event_date": "2026-11-05",
          "received_at": "2026-11-05T10:00:00Z",
          "visitor_key": "v1",
          "ip": "1.1.1.1",
          "country": "US",
          "region": "NY",
          "city": "NYC",
          "org": "ISP",
          "email": None,
          "percent_viewed": 0.5,
          "browser": "Chrome",
          "platform": "Windows",
          "mobile": False,
          "ingested_at": "2026-11-06T17:00:00Z",
      }],
  }

  summary = run_gold_pipeline(
      env="dev",
      spark=mock_spark,
      silver_data_by_endpoint=silver_data,
  )
  assert summary["record_counts"] == {
      "dim_media": 1,
      "dim_visitor": 1,
      "fact_media_engagement": 1,
  }
  assert summary["quarantine_counts"] == {
      "dim_media": 0,
      "dim_visitor": 0,
      "fact_media_engagement": 0,
  }
  assert summary["iceberg_upserted_counts"] == {
      "dim_media": 1,
      "dim_visitor": 1,
      "fact_media_engagement": 1,
  }

  # Sad path: bad records in Silver inputs are routed to Gold Quarantine on S3 without crashing
  with patch("scripts.gold.silver_to_gold.upload_quarantine_json_to_s3") as mock_q_upload:
    mock_q_upload.return_value = (
        "s3://wistia-pipeline-project/dev/quarantine/gold/dim_media/year=2026/month=11/day=06/batch.json"
    )
    dirty_silver_data = {
        "media_views": silver_data["media_views"],
        "media_engagement": silver_data["media_engagement"],
        "media_metadata": silver_data["media_metadata"] + [{"name": "Missing PK"}],
        "media_events": silver_data["media_events"] + [{"event_key": "bad_evt"}],
    }
    dirty_summary = run_gold_pipeline(
        env="dev",
        spark=mock_spark,
        silver_data_by_endpoint=dirty_silver_data,
    )
    assert dirty_summary["record_counts"]["dim_media"] == 1
    assert dirty_summary["quarantine_counts"] == {
        "dim_media": 1,
        "dim_visitor": 1,
        "fact_media_engagement": 1,
    }
    assert mock_q_upload.call_count == 3
    for call in mock_q_upload.call_args_list:
      assert call.kwargs["stage"] == "gold"

  # Sad path: neither silver_data_by_endpoint nor spark provided
  with pytest.raises(ValueError, match="Either `silver_data_by_endpoint` or an active `spark`"):
    run_gold_pipeline(env="dev", spark=None, silver_data_by_endpoint=None)


def test_glue_spark_helpers_and_orchestrator_happy_and_sad_path():
  cfg = build_iceberg_spark_configs("wistia-pipeline-project", "glue_catalog")
  assert cfg["spark.sql.catalog.glue_catalog.warehouse"] == "s3://wistia-pipeline-project/"

  with pytest.raises(ValueError, match="must be non-empty"):
    build_iceberg_spark_configs("", "glue_catalog")

  dt = parse_partition_date_arg("2026-11-05")
  assert dt is not None and dt.tzinfo == timezone.utc and dt.day == 5
  assert parse_partition_date_arg(None) is None

  with pytest.raises(ValueError, match="Invalid partition date"):
    parse_partition_date_arg("11-05-2026")

  with patch("scripts.aws.glue_spark.run_silver_pipeline") as mock_silver, patch(
      "scripts.aws.glue_spark.run_gold_pipeline"
  ) as mock_gold:
    mock_silver.return_value = [{"endpoint": "media_views", "silver_records": []}]
    mock_gold.return_value = {"record_counts": {"dim_media": 0}}
    mock_spark = MagicMock()

    res = run_glue_medallion_job(
        env="prod",
        partition_date_str="2026-11-05",
        spark=mock_spark,
    )
    assert res["env"] == "prod"
    mock_silver.assert_called_once()
    mock_gold.assert_called_once()
