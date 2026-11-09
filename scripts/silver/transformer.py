from typing import Any

SILVER_PRIMARY_KEYS: dict[str, str] = {
    "media_views": "hashed_id",
    "media_engagement": "media_id",
    "media_metadata": "hashed_id",
    "media_events": "event_key",
}


def transform_validated_records_for_silver(
    endpoint_name: str,
    valid_records: list[dict[str, Any]],
    media_id: str | None = None,
) -> list[dict[str, Any]]:
  """Flattens validated Bronze records into tabular Silver schemas ready for
  Apache Iceberg SCD Type 1 upsert."""
  if endpoint_name not in SILVER_PRIMARY_KEYS:
    raise ValueError(
        f"Unsupported endpoint {endpoint_name!r}. Expected one of {sorted(SILVER_PRIMARY_KEYS.keys())}"
    )

  transformed: list[dict[str, Any]] = []

  for r in valid_records:
    if endpoint_name == "media_views":
      stats = r.get("stats") or {}
      transformed.append({
          "id": r["id"],
          "hashed_id": r["hashed_id"],
          "name": r["name"],
          "page_loads": stats["pageLoads"],
          "visitors": stats["visitors"],
          "percent_of_visitors_clicking_play": stats["percentOfVisitorsClickingPlay"],
          "plays": stats["plays"],
          "average_percent_watched": stats["averagePercentWatched"],
          "ingested_at": r["ingested_at"],
      })

    elif endpoint_name == "media_engagement":
      resolved_media_id = r.get("media_id") or media_id
      if not resolved_media_id:
        raise ValueError(
            "media_engagement record requires 'media_id' either in payload or passed from S3 key."
        )
      transformed.append({
          "media_id": resolved_media_id,
          "engagement": r["engagement"],
          "engagement_data": r["engagement_data"],
          "rewatch_data": r.get("rewatch_data") or [],
          "ingested_at": r["ingested_at"],
      })

    elif endpoint_name == "media_metadata":
      thumb = r.get("thumbnail") or {}
      folder = r.get("folder") if isinstance(r.get("folder"), dict) else {}
      transformed.append({
          "id": r["id"],
          "hashed_id": r["hashed_id"],
          "name": r["name"],
          "type": r["type"],
          "archived": r["archived"],
          "duration": r["duration"],
          "created": r["created"],
          "updated": r["updated"],
          "description": r.get("description") or "",
          "protected": str(r["protected"]) if r.get("protected") is not None else None,
          "status": str(r["status"]) if r.get("status") is not None else None,
          "thumbnail_url": str(thumb["url"]) if thumb.get("url") else None,
          "thumbnail_width": thumb.get("width"),
          "thumbnail_height": thumb.get("height"),
          "folder_id": folder.get("id"),
          "folder_name": folder.get("name"),
          "ingested_at": r["ingested_at"],
      })

    elif endpoint_name == "media_events":
      ua = r.get("user_agent_details") or {}
      thumb = r.get("thumbnail") or {}
      received_str = str(r["received_at"])
      transformed.append({
          "event_key": r["event_key"],
          "received_at": received_str,
          "event_date": received_str[:10],
          "media_id": r["media_id"],
          "media_name": r["media_name"],
          "media_url": str(r["media_url"]),
          "visitor_key": r["visitor_key"],
          "ip": r.get("ip"),
          "country": r.get("country"),
          "region": r.get("region"),
          "city": r.get("city"),
          "lat": r.get("lat"),
          "lon": r.get("lon"),
          "org": r.get("org"),
          "email": r.get("email"),
          "percent_viewed": r["percent_viewed"],
          "embed_url": r.get("embed_url"),
          "conversion_type": r.get("conversion_type"),
          "iframe_heatmap_url": str(r["iframe_heatmap_url"]),
          "browser": ua.get("browser"),
          "browser_version": ua.get("browser_version"),
          "platform": ua.get("platform"),
          "mobile": ua.get("mobile"),
          "thumbnail_url": str(thumb["url"]) if thumb.get("url") else None,
          "ingested_at": r["ingested_at"],
      })

  return deduplicate_by_primary_key(endpoint_name, transformed)


def deduplicate_by_primary_key(
    endpoint_name: str,
    silver_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
  """Deduplicates a batch of Silver records by the endpoint's SCD Type 1
  primary key, retaining the latest record by `ingested_at`."""
  if endpoint_name not in SILVER_PRIMARY_KEYS:
    raise ValueError(f"Unsupported endpoint {endpoint_name!r}")

  pk_field = SILVER_PRIMARY_KEYS[endpoint_name]
  deduped: dict[Any, dict[str, Any]] = {}

  for row in silver_records:
    pk_val = row.get(pk_field)
    if pk_val is None:
      raise ValueError(
          f"Missing primary key field {pk_field!r} in Silver record for {endpoint_name!r}"
      )
    existing = deduped.get(pk_val)
    if existing is None or str(row.get("ingested_at", "")) >= str(
        existing.get("ingested_at", "")
    ):
      deduped[pk_val] = row

  return list(deduped.values())
