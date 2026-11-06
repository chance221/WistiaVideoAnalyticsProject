from datetime import datetime, timezone
import json
from typing import Any
import boto3
from botocore.exceptions import ClientError


def build_s3_bronze_key(
    endpoint_name: str,
    media_id: str,
    ingested_at: datetime,
    layer_prefix: str = "bronze",
) -> str:
  utc_time = ingested_at.astimezone(timezone.utc)
  year = utc_time.strftime("%Y")
  month = utc_time.strftime("%m")
  day = utc_time.strftime("%d")
  timestamp_str = utc_time.strftime("%Y%m%dT%H%M%SZ")

  return (
      f"{layer_prefix}/{endpoint_name}/"
      f"year={year}/month={month}/day={day}/"
      f"{media_id}_{timestamp_str}.json"
  )


def upload_raw_json_to_s3(
    bucket_name: str,
    region: str,
    endpoint_name: str,
    media_id: str,
    data: Any,
    ingested_at: datetime | None = None,
) -> str:
  if ingested_at is None:
    ingested_at = datetime.now(timezone.utc)

  s3_key = build_s3_bronze_key(
      endpoint_name=endpoint_name,
      media_id=media_id,
      ingested_at=ingested_at,
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
    quarantined_at: datetime | None = None,
) -> str:
  if quarantined_at is None:
    quarantined_at = datetime.now(timezone.utc)

  s3_key = build_s3_bronze_key(
      endpoint_name=endpoint_name,
      media_id=media_id,
      ingested_at=quarantined_at,
      layer_prefix="quarantine",
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

