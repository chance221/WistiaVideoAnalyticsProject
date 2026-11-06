from datetime import datetime, timezone
from typing import Any
from pydantic import ValidationError
from scripts.aws.s3_client import upload_quarantine_json_to_s3
from scripts.models.config_models import ENDPOINT_MODEL_MAP


def validate_bronze_records(
    endpoint_name: str,
    raw_payload: dict[str, Any] | list[Any],
    validated_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
  """Validates Bronze JSON records against the endpoint's Pydantic model.

  Returns a tuple of (valid_records, quarantine_records):
  - valid_records: list of serialized dicts ready for Silver Parquet transformation.
  - quarantine_records: list of dicts containing raw_record, endpoint, error details,
    and timestamp ready for S3 Quarantine storage.
  """
  if endpoint_name not in ENDPOINT_MODEL_MAP:
    raise ValueError(
        f"Unknown endpoint {endpoint_name!r}. Expected one of {sorted(ENDPOINT_MODEL_MAP.keys())}"
    )

  if validated_at is None:
    validated_at = datetime.now(timezone.utc)

  ts_iso = validated_at.astimezone(timezone.utc).isoformat()
  model_cls = ENDPOINT_MODEL_MAP[endpoint_name]

  if isinstance(raw_payload, dict):
    records = [raw_payload]
  elif isinstance(raw_payload, list):
    records = raw_payload
  else:
    raise TypeError(
        f"Expected dict or list for raw_payload, got {type(raw_payload).__name__}"
    )

  valid_records: list[dict[str, Any]] = []
  quarantine_records: list[dict[str, Any]] = []

  for record in records:
    if not isinstance(record, dict):
      quarantine_records.append({
          "endpoint": endpoint_name,
          "raw_record": record,
          "errors": [{"msg": f"Expected object/dict record, got {type(record).__name__}"}],
          "quarantined_at": ts_iso,
      })
      continue

    try:
      validated_obj = model_cls.model_validate(record)
      serialized = validated_obj.model_dump(mode="json")
      serialized["ingested_at"] = ts_iso
      valid_records.append(serialized)
    except ValidationError as exc:
      quarantine_records.append({
          "endpoint": endpoint_name,
          "raw_record": record,
          "errors": exc.errors(include_url=False),
          "quarantined_at": ts_iso,
      })

  return valid_records, quarantine_records


def process_and_quarantine_invalid_records(
    bucket_name: str,
    region: str,
    endpoint_name: str,
    media_id: str,
    raw_payload: dict[str, Any] | list[Any],
    validated_at: datetime | None = None,
) -> dict[str, Any]:
  """Validates Bronze records for an endpoint and uploads any invalid records
  to the S3 Quarantine layer (`s3://<bucket>/quarantine/<endpoint>/...`)."""
  if validated_at is None:
    validated_at = datetime.now(timezone.utc)

  valid_records, quarantine_records = validate_bronze_records(
      endpoint_name=endpoint_name,
      raw_payload=raw_payload,
      validated_at=validated_at,
  )

  quarantine_s3_uri: str | None = None
  if quarantine_records:
    quarantine_s3_uri = upload_quarantine_json_to_s3(
        bucket_name=bucket_name,
        region=region,
        endpoint_name=endpoint_name,
        media_id=media_id,
        quarantine_records=quarantine_records,
        quarantined_at=validated_at,
    )

  return {
      "endpoint": endpoint_name,
      "media_id": media_id,
      "valid_records": valid_records,
      "quarantine_records": quarantine_records,
      "quarantine_s3_uri": quarantine_s3_uri,
  }
