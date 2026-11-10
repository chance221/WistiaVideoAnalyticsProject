from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

from scripts.aws.s3_client import (
    list_bronze_s3_keys,
    parse_media_id_from_s3_key,
    read_json_from_s3,
)
from scripts.models.config_loader import load_environment_config, load_media_config
from scripts.silver.transformer import (
    SILVER_PRIMARY_KEYS,
    SILVER_SPARK_SCHEMAS,
    deduplicate_by_primary_key,
    transform_validated_records_for_silver,
)
from scripts.silver.validator import process_and_quarantine_invalid_records

CUMULATIVE_MEDIA_ENDPOINTS = frozenset({
    "media_views",
    "media_engagement",
    "media_metadata",
})


def select_latest_s3_keys_per_media(s3_keys: list[str]) -> list[str]:
  """For cumulative video-level endpoints (`media_views`, `media_engagement`,
  `media_metadata`), selects only the single newest S3 key per `media_id`
  based on the `<media_id>_<YYYYMMDDTHHMMSSZ>.json` filename timestamp."""
  if not isinstance(s3_keys, list):
    raise TypeError("s3_keys must be a list of S3 key strings")

  latest_by_media: dict[str, tuple[str, str]] = {}
  for s3_key in s3_keys:
    media_id = parse_media_id_from_s3_key(s3_key)
    filename = PurePosixPath(s3_key).stem
    timestamp_part = filename.rsplit("_", 1)[1]
    existing = latest_by_media.get(media_id)
    if existing is None or (timestamp_part, s3_key) >= (existing[0], existing[1]):
      latest_by_media[media_id] = (timestamp_part, s3_key)

  return sorted(item[1] for item in latest_by_media.values())


def build_iceberg_scd1_merge_sql(
    catalog_name: str,
    database_name: str,
    table_name: str,
    source_view_name: str,
    primary_key: str,
) -> str:
  """Builds an Apache Iceberg SCD Type 1 MERGE INTO SQL statement."""
  if not all([catalog_name, database_name, table_name, source_view_name, primary_key]):
    raise ValueError(
        "catalog_name, database_name, table_name, source_view_name, and primary_key must all be non-empty."
    )

  full_table_name = f"{catalog_name}.{database_name}.{table_name}"
  return (
      f"MERGE INTO {full_table_name} AS target "
      f"USING {source_view_name} AS source "
      f"ON target.{primary_key} = source.{primary_key} "
      f"WHEN MATCHED THEN UPDATE SET * "
      f"WHEN NOT MATCHED THEN INSERT *"
  )


def upsert_silver_iceberg_table(
    spark: Any,
    records: list[dict[str, Any]],
    endpoint_name: str,
    bucket_name: str,
    env: str = "dev",
    database_name: str = "wistia_silver",
    catalog_name: str = "glue_catalog",
) -> int:
  """Creates or SCD1-merges Silver records into an Apache Iceberg table in S3."""
  if endpoint_name not in SILVER_PRIMARY_KEYS:
    raise ValueError(f"Unsupported endpoint {endpoint_name!r}")

  if not records:
    return 0

  clean_env = env.strip("/ ")
  pk_field = SILVER_PRIMARY_KEYS[endpoint_name]
  full_db_name = f"{catalog_name}.{database_name}_{clean_env}"
  full_table_name = f"{full_db_name}.{endpoint_name}"
  source_view_name = f"source_{endpoint_name}"
  table_location = f"s3://{bucket_name}/{clean_env}/silver/{endpoint_name}"

  spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_db_name}")

  df = spark.createDataFrame(records, schema=SILVER_SPARK_SCHEMAS[endpoint_name])
  df.createOrReplaceTempView(source_view_name)

  if not spark.catalog.tableExists(full_table_name):
    (
        df.writeTo(full_table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("location", table_location)
        .create()
    )
  else:
    merge_sql = build_iceberg_scd1_merge_sql(
        catalog_name=catalog_name,
        database_name=f"{database_name}_{clean_env}",
        table_name=endpoint_name,
        source_view_name=source_view_name,
        primary_key=pk_field,
    )
    spark.sql(merge_sql)

  return len(records)


def process_bronze_endpoint_to_silver(
    bucket_name: str,
    region: str,
    endpoint_name: str,
    env: str = "dev",
    partition_date: datetime | None = None,
    spark: Any | None = None,
    database_name: str = "wistia_silver",
    catalog_name: str = "glue_catalog",
) -> dict[str, Any]:
  """Reads Bronze S3 JSON files for an endpoint, validates with Pydantic,
  routes invalid records to Quarantine, transforms valid records to Silver schema,
  and performs an SCD Type 1 upsert into Apache Iceberg if a SparkSession is provided."""
  if endpoint_name not in SILVER_PRIMARY_KEYS:
    raise ValueError(f"Unsupported endpoint {endpoint_name!r}")

  s3_keys = list_bronze_s3_keys(
      bucket_name=bucket_name,
      region=region,
      endpoint_name=endpoint_name,
      env=env,
      partition_date=partition_date,
  )

  if endpoint_name in CUMULATIVE_MEDIA_ENDPOINTS and partition_date is None:
    s3_keys = select_latest_s3_keys_per_media(s3_keys)

  all_silver_rows: list[dict[str, Any]] = []
  total_quarantined = 0
  quarantine_uris: list[str] = []

  for s3_key in s3_keys:
    media_id = parse_media_id_from_s3_key(s3_key)
    raw_payload = read_json_from_s3(
        bucket_name=bucket_name,
        region=region,
        s3_key=s3_key,
    )

    validation_result = process_and_quarantine_invalid_records(
        bucket_name=bucket_name,
        region=region,
        endpoint_name=endpoint_name,
        media_id=media_id,
        raw_payload=raw_payload,
        env=env,
    )

    valid_records = validation_result["valid_records"]
    quarantine_records = validation_result["quarantine_records"]
    total_quarantined += len(quarantine_records)

    if validation_result["quarantine_s3_uri"]:
      quarantine_uris.append(validation_result["quarantine_s3_uri"])

    if valid_records:
      silver_rows = transform_validated_records_for_silver(
          endpoint_name=endpoint_name,
          valid_records=valid_records,
          media_id=media_id,
      )
      all_silver_rows.extend(silver_rows)

  deduped_silver_rows = deduplicate_by_primary_key(endpoint_name, all_silver_rows)

  upserted_count = 0
  if spark is not None and deduped_silver_rows:
    upserted_count = upsert_silver_iceberg_table(
        spark=spark,
        records=deduped_silver_rows,
        endpoint_name=endpoint_name,
        bucket_name=bucket_name,
        env=env,
        database_name=database_name,
        catalog_name=catalog_name,
    )

  return {
      "endpoint": endpoint_name,
      "env": env,
      "files_processed": len(s3_keys),
      "silver_records": deduped_silver_rows,
      "silver_record_count": len(deduped_silver_rows),
      "quarantine_record_count": total_quarantined,
      "quarantine_s3_uris": quarantine_uris,
      "iceberg_upserted_count": upserted_count,
  }


def run_silver_pipeline(
    env: str = "dev",
    media_config_name: str = "media",
    partition_date: datetime | None = None,
    spark: Any | None = None,
    database_name: str = "wistia_silver",
    catalog_name: str = "glue_catalog",
) -> list[dict[str, Any]]:
  """Runs the Bronze-to-Silver validation, quarantine, and SCD1 Iceberg pipeline
  across all configured endpoints."""
  env_config = load_environment_config(env)
  media_config = load_media_config(media_config_name)

  results: list[dict[str, Any]] = []
  for endpoint_name in media_config.endpoints:
    summary = process_bronze_endpoint_to_silver(
        bucket_name=env_config.aws.bucket_name,
        region=env_config.aws.region,
        endpoint_name=endpoint_name,
        env=env_config.env,
        partition_date=partition_date,
        spark=spark,
        database_name=database_name,
        catalog_name=catalog_name,
    )
    results.append(summary)

  return results
