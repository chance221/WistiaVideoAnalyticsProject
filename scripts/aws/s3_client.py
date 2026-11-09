from datetime import datetime, timezone
import json
from pathlib import PurePosixPath
from typing import Any
import boto3
from botocore.exceptions import ClientError


def parse_media_id_from_s3_key(s3_key: str) -> str:
  """Extracts `<media_id>` from `.../<media_id>_<YYYYMMDDTHHMMSSZ>.json`."""
  filename = PurePosixPath(s3_key).name
  if not filename.endswith(".json") or "_" not in filename:
    raise ValueError(
        f"Invalid Bronze S3 key filename format {filename!r}. Expected '<media_id>_<timestamp>.json'."
    )
  media_id = filename.rsplit("_", 1)[0]
  if not media_id:
    raise ValueError(f"Empty media_id in S3 key {s3_key!r}")
  return media_id


def build_s3_bronze_key(
    endpoint_name: str,
    media_id: str,
    ingested_at: datetime,
    env: str = "dev",
    layer_prefix: str = "bronze",
) -> str:
  """Builds an environment-prefixed Hive-partitioned S3 key:
  `<env>/<layer_prefix>/<endpoint_name>/year=YYYY/month=MM/day=DD/<media_id>_<YYYYMMDDTHHMMSSZ>.json`
  """
  clean_env = env.strip("/ ")
  if not clean_env:
    raise ValueError("env must be a non-empty string (e.g. 'dev' or 'prod')")

  utc_time = ingested_at.astimezone(timezone.utc)
  year = utc_time.strftime("%Y")
  month = utc_time.strftime("%m")
  day = utc_time.strftime("%d")
  timestamp_str = utc_time.strftime("%Y%m%dT%H%M%SZ")

  return (
      f"{clean_env}/{layer_prefix}/{endpoint_name}/"
      f"year={year}/month={month}/day={day}/"
      f"{media_id}_{timestamp_str}.json"
  )


def upload_raw_json_to_s3(
    bucket_name: str,
    region: str,
    endpoint_name: str,
    media_id: str,
    data: Any,
    env: str = "dev",
    ingested_at: datetime | None = None,
) -> str:
  if ingested_at is None:
    ingested_at = datetime.now(timezone.utc)

  s3_key = build_s3_bronze_key(
      endpoint_name=endpoint_name,
      media_id=media_id,
      ingested_at=ingested_at,
      env=env,
      layer_prefix="bronze",
  )

  session = boto3.session.Session()
  s3_client = session.client("s3", region_name=region)

  body_bytes = json.dumps(data, indent=2).encode("utf-8")

  try:
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=body_bytes,
        ContentType="application/json",
        Metadata={
            "env": env,
            "ingested_at": ingested_at.astimezone(timezone.utc).isoformat(),
            "endpoint": endpoint_name,
            "media_id": media_id,
        },
    )
  except ClientError as exc:
    raise RuntimeError(
        f"Failed to upload raw JSON to s3://{bucket_name}/{s3_key}: {exc}"
    ) from exc

  return f"s3://{bucket_name}/{s3_key}"


def upload_quarantine_json_to_s3(
    bucket_name: str,
    region: str,
    endpoint_name: str,
    media_id: str,
    quarantine_records: list[dict[str, Any]],
    env: str = "dev",
    stage: str = "silver",
    quarantined_at: datetime | None = None,
) -> str:
  clean_stage = stage.strip("/ ").lower()
  if clean_stage not in ("silver", "gold"):
    raise ValueError(
        f"Invalid quarantine stage {stage!r}. Expected 'silver' or 'gold'."
    )

  if quarantined_at is None:
    quarantined_at = datetime.now(timezone.utc)

  s3_key = build_s3_bronze_key(
      endpoint_name=endpoint_name,
      media_id=media_id,
      ingested_at=quarantined_at,
      env=env,
      layer_prefix=f"quarantine/{clean_stage}",
  )

  session = boto3.session.Session()
  s3_client = session.client("s3", region_name=region)

  body_bytes = json.dumps(quarantine_records, indent=2).encode("utf-8")

  try:
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=body_bytes,
        ContentType="application/json",
        Metadata={
            "env": env,
            "stage": clean_stage,
            "quarantined_at": quarantined_at.astimezone(timezone.utc).isoformat(),
            "endpoint": endpoint_name,
            "media_id": media_id,
            "quarantine_count": str(len(quarantine_records)),
        },
    )
  except ClientError as exc:
    raise RuntimeError(
        f"Failed to upload quarantine JSON to s3://{bucket_name}/{s3_key}: {exc}"
    ) from exc

  return f"s3://{bucket_name}/{s3_key}"


def list_bronze_s3_keys(
    bucket_name: str,
    region: str,
    endpoint_name: str,
    env: str = "dev",
    partition_date: datetime | None = None,
    layer_prefix: str = "bronze",
) -> list[str]:
  """Lists all `.json` object keys in S3 under `<env>/bronze/<endpoint_name>/`
  (optionally scoped to `year=YYYY/month=MM/day=DD/`)."""
  clean_env = env.strip("/ ")
  if not clean_env:
    raise ValueError("env must be a non-empty string")

  if partition_date is not None:
    utc_dt = partition_date.astimezone(timezone.utc)
    prefix = (
        f"{clean_env}/{layer_prefix}/{endpoint_name}/"
        f"year={utc_dt:%Y}/month={utc_dt:%m}/day={utc_dt:%d}/"
    )
  else:
    prefix = f"{clean_env}/{layer_prefix}/{endpoint_name}/"

  session = boto3.session.Session()
  s3_client = session.client("s3", region_name=region)

  keys: list[str] = []
  paginator = s3_client.get_paginator("list_objects_v2")
  try:
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
      for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".json"):
          keys.append(key)
  except ClientError as exc:
    raise RuntimeError(
        f"Failed to list Bronze keys in s3://{bucket_name}/{prefix}: {exc}"
    ) from exc

  return sorted(keys)


def read_json_from_s3(
    bucket_name: str,
    region: str,
    s3_key: str,
) -> Any:
  """Reads and parses a JSON file from S3."""
  session = boto3.session.Session()
  s3_client = session.client("s3", region_name=region)

  try:
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    raw_bytes = response["Body"].read()
    return json.loads(raw_bytes.decode("utf-8"))
  except (ClientError, json.JSONDecodeError) as exc:
    raise RuntimeError(
        f"Failed to read JSON from s3://{bucket_name}/{s3_key}: {exc}"
    ) from exc
