from datetime import datetime, timezone
import os
from typing import Any

from scripts.aws.s3_client import upload_raw_json_to_s3
from scripts.aws.secrets_manager import get_secret
from scripts.models.config_loader import load_environment_config, load_media_config
from scripts.raw.get_json_data import (
    build_url,
    create_paged_params,
    fetch_paginated_data,
)


def run_raw_ingestion(
    env: str = "dev",
    media_config_name: str = "media",
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
  """Orchestrates fetching raw JSON data for all configured Wistia endpoints
  and media IDs, and writes unaltered JSON payloads to the S3 Bronze layer."""
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
          ingested_at=ingested_at,
      )

      record_count = len(raw_data) if isinstance(raw_data, list) else 1
      print(f"Uploaded {endpoint_name} ({media_id}) [{record_count} record(s)] -> {s3_uri}")

      ingestion_results.append({
          "endpoint": endpoint_name,
          "media_id": media_id,
          "record_count": record_count,
          "s3_uri": s3_uri,
          "ingested_at": ingested_at.isoformat(),
      })

  return ingestion_results


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
  """AWS Lambda entry point triggered by Amazon EventBridge."""
  event = event or {}
  env = event.get("env") or os.getenv("ENV", "dev")
  start_date = event.get("start_date")
  end_date = event.get("end_date")

  results = run_raw_ingestion(
      env=env,
      start_date=start_date,
      end_date=end_date,
  )

  return {
      "statusCode": 200,
      "env": env,
      "files_uploaded": len(results),
      "results": results,
  }


if __name__ == "__main__":
  env_name = os.getenv("ENV", "dev")
  summary = run_raw_ingestion(env=env_name)
  print(f"\nCompleted raw ingestion: {len(summary)} files written to S3.")
