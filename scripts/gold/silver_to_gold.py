from typing import Any

from scripts.aws.s3_client import upload_quarantine_json_to_s3
from scripts.gold.transformer import (
    GOLD_PRIMARY_KEYS,
    GOLD_SPARK_SCHEMAS,
    validate_and_build_gold_tables,
)
from scripts.models.config_loader import load_environment_config
from scripts.silver.bronze_to_silver import build_iceberg_scd1_merge_sql
from scripts.silver.transformer import SILVER_PRIMARY_KEYS


def upsert_gold_iceberg_table(
    spark: Any,
    records: list[dict[str, Any]],
    table_name: str,
    bucket_name: str,
    env: str = "dev",
    database_name: str = "wistia_gold",
    catalog_name: str = "glue_catalog",
) -> int:
  """Creates or SCD1-merges Gold records (`dim_media`, `dim_visitor`, `fact_media_engagement`)
  into an Apache Iceberg table in S3 (`s3://<bucket>/<env>/gold/<table_name>`)."""
  if table_name not in GOLD_PRIMARY_KEYS:
    raise ValueError(
        f"Unsupported Gold table {table_name!r}. Expected one of {sorted(GOLD_PRIMARY_KEYS.keys())}"
    )

  if not records:
    return 0

  clean_env = env.strip("/ ")
  pk_field = GOLD_PRIMARY_KEYS[table_name]
  full_db_name = f"{catalog_name}.{database_name}_{clean_env}"
  full_table_name = f"{full_db_name}.{table_name}"
  source_view_name = f"source_gold_{table_name}"
  table_location = f"s3://{bucket_name}/{clean_env}/gold/{table_name}"

  spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_db_name}")

  df = spark.createDataFrame(records, schema=GOLD_SPARK_SCHEMAS[table_name])
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
        table_name=table_name,
        source_view_name=source_view_name,
        primary_key=pk_field,
    )
    spark.sql(merge_sql)

  return len(records)


def read_silver_table_records(
    spark: Any,
    endpoint_name: str,
    env: str = "dev",
    database_name: str = "wistia_silver",
    catalog_name: str = "glue_catalog",
) -> list[dict[str, Any]]:
  """Reads records from a Silver Iceberg table into Python dicts."""
  if endpoint_name not in SILVER_PRIMARY_KEYS:
    raise ValueError(f"Unsupported Silver table {endpoint_name!r}")

  clean_env = env.strip("/ ")
  full_table_name = f"{catalog_name}.{database_name}_{clean_env}.{endpoint_name}"
  if not spark.catalog.tableExists(full_table_name):
    return []

  rows = spark.table(full_table_name).collect()
  return [row.asDict(recursive=True) for row in rows]


def run_gold_pipeline(
    env: str = "dev",
    spark: Any | None = None,
    silver_data_by_endpoint: dict[str, list[dict[str, Any]]] | None = None,
    silver_database: str = "wistia_silver",
    gold_database: str = "wistia_gold",
    catalog_name: str = "glue_catalog",
) -> dict[str, Any]:
  """Transforms Silver records into Gold dimensional and fact tables (`dim_media`,
  `dim_visitor`, `fact_media_engagement`) and performs SCD Type 1 Iceberg upserts."""
  env_config = load_environment_config(env)

  if silver_data_by_endpoint is None:
    if spark is None:
      raise ValueError(
          "Either `silver_data_by_endpoint` or an active `spark` session must be provided."
      )
    silver_data_by_endpoint = {
        ep: read_silver_table_records(
            spark=spark,
            endpoint_name=ep,
            env=env_config.env,
            database_name=silver_database,
            catalog_name=catalog_name,
        )
        for ep in SILVER_PRIMARY_KEYS
    }

  silver_views = silver_data_by_endpoint.get("media_views", [])
  silver_engagement = silver_data_by_endpoint.get("media_engagement", [])
  silver_metadata = silver_data_by_endpoint.get("media_metadata", [])
  silver_events = silver_data_by_endpoint.get("media_events", [])

  gold_tables, quarantine_by_table = validate_and_build_gold_tables(
      silver_views=silver_views,
      silver_engagement=silver_engagement,
      silver_metadata=silver_metadata,
      silver_events=silver_events,
  )

  quarantine_s3_uris: dict[str, str | None] = {}
  for table_name, q_records in quarantine_by_table.items():
    if q_records:
      quarantine_s3_uris[table_name] = upload_quarantine_json_to_s3(
          bucket_name=env_config.aws.bucket_name,
          region=env_config.aws.region,
          endpoint_name=table_name,
          media_id="batch",
          quarantine_records=q_records,
          env=env_config.env,
          stage="gold",
      )
    else:
      quarantine_s3_uris[table_name] = None

  upserted_counts: dict[str, int] = {}
  for table_name, rows in gold_tables.items():
    if spark is not None and rows:
      upserted_counts[table_name] = upsert_gold_iceberg_table(
          spark=spark,
          records=rows,
          table_name=table_name,
          bucket_name=env_config.aws.bucket_name,
          env=env_config.env,
          database_name=gold_database,
          catalog_name=catalog_name,
      )
    else:
      upserted_counts[table_name] = 0

  return {
      "env": env_config.env,
      "tables": gold_tables,
      "record_counts": {k: len(v) for k, v in gold_tables.items()},
      "quarantine_counts": {k: len(v) for k, v in quarantine_by_table.items()},
      "quarantine_s3_uris": quarantine_s3_uris,
      "iceberg_upserted_counts": upserted_counts,
  }
