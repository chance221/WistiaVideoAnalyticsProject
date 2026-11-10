from datetime import datetime, timezone
import sys
from typing import Any

from scripts.gold.silver_to_gold import run_gold_pipeline
from scripts.models.config_loader import load_environment_config
from scripts.silver.bronze_to_silver import run_silver_pipeline


def build_iceberg_spark_configs(
    bucket_name: str,
    catalog_name: str = "glue_catalog",
) -> dict[str, str]:
  """Returns the Spark configuration properties required for Apache Iceberg
  backed by the AWS Glue Data Catalog and S3."""
  if not bucket_name or not catalog_name:
    raise ValueError("bucket_name and catalog_name must be non-empty")

  return {
      "spark.sql.extensions": (
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      ),
      f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
      f"spark.sql.catalog.{catalog_name}.catalog-impl": (
          "org.apache.iceberg.aws.glue.GlueCatalog"
      ),
      f"spark.sql.catalog.{catalog_name}.io-impl": (
          "org.apache.iceberg.aws.s3.S3FileIO"
      ),
      f"spark.sql.catalog.{catalog_name}.warehouse": f"s3://{bucket_name}/",
  }


def create_iceberg_spark_session(
    bucket_name: str,
    app_name: str = "WistiaMedallionIcebergJob",
    catalog_name: str = "glue_catalog",
) -> Any:
  """Initializes a PySpark SparkSession configured for Apache Iceberg on AWS Glue."""
  from pyspark.sql import SparkSession  # type: ignore[import-not-found]

  configs = build_iceberg_spark_configs(
      bucket_name=bucket_name,
      catalog_name=catalog_name,
  )
  builder = SparkSession.builder.appName(app_name)
  for k, v in configs.items():
    builder = builder.config(k, v)

  return builder.getOrCreate()


def parse_partition_date_arg(date_str: str | None) -> datetime | None:
  """Parses an optional 'YYYY-MM-DD' partition date argument into a UTC datetime."""
  if not date_str:
    return None
  try:
    dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)
  except ValueError as exc:
    raise ValueError(
        f"Invalid partition date {date_str!r}. Expected 'YYYY-MM-DD'."
    ) from exc


def run_glue_medallion_job(
    env: str = "prod",
    partition_date_str: str | None = None,
    spark: Any | None = None,
    catalog_name: str = "glue_catalog",
    stage: str = "all",
) -> dict[str, Any]:
  """End-to-end AWS Glue job orchestrator supporting `--STAGE` ('all', 'silver', 'gold'):
  - 'all' (default): Runs Bronze -> Silver -> Gold in a single warm Spark session.
  - 'silver': Runs only Bronze -> Silver (+ Silver Quarantine).
  - 'gold': Runs only Silver -> Gold (+ Gold Quarantine) reading directly from Silver Iceberg tables.
  """
  clean_stage = stage.strip().lower()
  if clean_stage not in ("all", "silver", "gold"):
    raise ValueError(
        f"Invalid stage {stage!r}. Expected 'all', 'silver', or 'gold'."
    )

  env_config = load_environment_config(env)
  partition_date = parse_partition_date_arg(partition_date_str)

  if spark is None:
    spark = create_iceberg_spark_session(
        bucket_name=env_config.aws.bucket_name,
        catalog_name=catalog_name,
    )

  silver_summaries: list[dict[str, Any]] | None = None
  gold_summary: dict[str, Any] | None = None

  if clean_stage in ("all", "silver"):
    silver_summaries = run_silver_pipeline(
        env=env,
        partition_date=partition_date,
        spark=spark,
        catalog_name=catalog_name,
    )

  if clean_stage == "all":
    assert silver_summaries is not None
    silver_data_by_endpoint = {
        item["endpoint"]: item["silver_records"] for item in silver_summaries
    }
    gold_summary = run_gold_pipeline(
        env=env,
        spark=spark,
        silver_data_by_endpoint=silver_data_by_endpoint,
        catalog_name=catalog_name,
    )
  elif clean_stage == "gold":
    gold_summary = run_gold_pipeline(
        env=env,
        spark=spark,
        silver_data_by_endpoint=None,
        catalog_name=catalog_name,
    )

  return {
      "env": env,
      "stage": clean_stage,
      "silver": silver_summaries,
      "gold": gold_summary,
  }


if __name__ == "__main__":
  env_arg = "prod"
  date_arg = None
  stage_arg = "all"
  for i, arg in enumerate(sys.argv):
    if arg == "--ENV" and i + 1 < len(sys.argv):
      env_arg = sys.argv[i + 1]
    elif arg == "--PARTITION_DATE" and i + 1 < len(sys.argv):
      date_arg = sys.argv[i + 1]
    elif arg == "--STAGE" and i + 1 < len(sys.argv):
      stage_arg = sys.argv[i + 1]

  run_glue_medallion_job(
      env=env_arg,
      partition_date_str=date_arg,
      stage=stage_arg,
  )
