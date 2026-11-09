from datetime import datetime, timedelta, timezone
import os
from typing import Any

from scripts.aws.s3_client import upload_raw_json_to_s3
from scripts.aws.secrets_manager import get_secret
from scripts.models.config_loader import load_environment_config, load_media_config
from scripts.raw.get_json_data import (
    build_url,
    create_paged_params,
    fetch_paginated_data,
    validate_date_tuple,
)


def resolve_ingestion_date_window(
    event: dict[str, Any] | None = None,
    reference_time: datetime | None = None,
) -> tuple[str | None, str | None]:
  """Resolves the (start_date, end_date) query parameters for Wistia ingestion:
  1. If `event.get("full_load") is True`, returns `(None, None)` for a full historical load.
  2. If `start_date` and `end_date` are provided in `event` (or `event["queryStringParameters"]`),
     validates that both are 'YYYY-MM-DD' and `start_date <= end_date`, and returns them.
  3. Otherwise (e.g. daily EventBridge scheduled trigger), defaults to yesterday's UTC date
     (`YYYY-MM-DD`) relative to `event["time"]` (if present) or `reference_time` / `datetime.now(timezone.utc)`.
  """
  event = event or {}
  query_params = event.get("queryStringParameters") or {}
  if not isinstance(query_params, dict):
    query_params = {}

  if event.get("full_load") is True or query_params.get("full_load") in (True, "true", "True"):
    return None, None

  start_date = event.get("start_date") or query_params.get("start_date")
  end_date = event.get("end_date") or query_params.get("end_date")

  if start_date or end_date:
    if not (start_date and end_date):
      raise ValueError(
          "Both 'start_date' and 'end_date' must be provided together when specifying a custom date window."
      )
    validate_date_tuple(( str(start_date), str(end_date) ))
    return str(start_date), str(end_date)

  if reference_time is None:
    eb_time_str = event.get("time")
    if isinstance(eb_time_str, str) and eb_time_str.strip():
      try:
        reference_time = datetime.fromisoformat(
            eb_time_str.strip().replace("Z", "+00:00")
        )
      except ValueError as exc:
        raise ValueError(
            f"Invalid EventBridge 'time' timestamp {eb_time_str!r}"
        ) from exc
    else:
      reference_time = datetime.now(timezone.utc)

  yesterday_str = (
      reference_time.astimezone(timezone.utc) - timedelta(days=1)
  ).strftime("%Y-%m-%d")
  return yesterday_str, yesterday_str


def run_raw_ingestion(
    env: str = "dev",
    media_config_name: str = "media",
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
  """Orchestrates fetching raw JSON data for all configured Wistia endpoints
  and media IDs, and writes unaltered JSON payloads to `<env>/bronze/...` in S3."""
  if (start_date and not end_date) or (end_date and not start_date):
    raise ValueError("Both 'start_date' and 'end_date' must be provided together.")
  if start_date and end_date:
    validate_date_tuple((start_date, end_date))

  env_config = load_environment_config(env)
  media_config = load_media_config(media_config_name)
  api_key = get_secret(env_config.aws)

  ingested_at = datetime.now(timezone.utc)
  ingestion_results: list[dict[str, Any]] = []

  for endpoint_name, endpoint_cfg in media_config.endpoints.items():
    is_path_scoped = "{mediaHashedId}" in endpoint_cfg.url_pattern

    for media_id in media_config.media_ids:
      url = build_url(
          api_base=media_config.api_base,
          url_pattern=endpoint_cfg.url_pattern,
          media_hash_id=media_id,
      )

      base_params = create_paged_params(
          media_id=None if is_path_scoped else media_id,
          start_date=start_date,
          end_date=end_date,
      )

      raw_data = fetch_paginated_data(
          url=url,
          api_key=api_key,
          per_page=media_config.results_per_page,
          base_params=base_params,
      )

      s3_uri = upload_raw_json_to_s3(
          bucket_name=env_config.aws.bucket_name,
          region=env_config.aws.region,
          endpoint_name=endpoint_name,
          media_id=media_id,
          data=raw_data,
          env=env_config.env,
          ingested_at=ingested_at,
      )

      record_count = len(raw_data) if isinstance(raw_data, list) else 1
      print(
          f"Uploaded {endpoint_name} ({media_id}) [{record_count} record(s)] -> {s3_uri}"
      )

      ingestion_results.append({
          "endpoint": endpoint_name,
          "media_id": media_id,
          "record_count": record_count,
          "start_date": start_date,
          "end_date": end_date,
          "s3_uri": s3_uri,
          "ingested_at": ingested_at.isoformat(),
      })

  return ingestion_results


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
  """AWS Lambda entry point triggered by Amazon EventBridge."""
  event = event or {}
  env = event.get("env") or os.getenv("ENV", "dev")
  start_date, end_date = resolve_ingestion_date_window(event)

  results = run_raw_ingestion(
      env=env,
      start_date=start_date,
      end_date=end_date,
  )

  return {
      "statusCode": 200,
      "env": env,
      "start_date": start_date,
      "end_date": end_date,
      "files_uploaded": len(results),
      "results": results,
  }


if __name__ == "__main__":
  env_name = os.getenv("ENV", "dev")
  start_dt, end_dt = resolve_ingestion_date_window({})
  summary = run_raw_ingestion(env=env_name, start_date=start_dt, end_date=end_dt)
  print(f"\nCompleted raw ingestion ({start_dt} to {end_dt}): {len(summary)} files written to S3.")
