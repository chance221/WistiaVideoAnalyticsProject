from typing import Any

SILVER_PRIMARY_KEYS: dict[str, str] = {
    "media_views": "hashed_id",
    "media_engagement": "media_id",
    "media_metadata": "hashed_id",
    "media_events": "event_key",
}

SILVER_SPARK_SCHEMAS: dict[str, str] = {
    "media_views": (
        "id BIGINT, hashed_id STRING, name STRING, page_loads BIGINT, "
        "visitors BIGINT, percent_of_visitors_clicking_play BIGINT, "
        "plays BIGINT, average_percent_watched BIGINT, ingested_at STRING"
    ),
    "media_engagement": (
        "media_id STRING, engagement DOUBLE, engagement_data ARRAY<BIGINT>, "
        "rewatch_data ARRAY<BIGINT>, ingested_at STRING"
    ),
    "media_metadata": (
        "id BIGINT, hashed_id STRING, name STRING, type STRING, "
        "archived BOOLEAN, duration DOUBLE, created STRING, updated STRING, "
        "description STRING, protected STRING, status STRING, "
        "thumbnail_url STRING, thumbnail_width BIGINT, thumbnail_height BIGINT, "
        "folder_id BIGINT, folder_name STRING, ingested_at STRING"
    ),
    "media_events": (
        "event_key STRING, received_at STRING, event_date STRING, "
        "media_id STRING, media_name STRING, media_url STRING, "
        "visitor_key STRING, ip STRING, country STRING, region STRING, "
        "city STRING, lat DOUBLE, lon DOUBLE, org STRING, email STRING, "
        "percent_viewed DOUBLE, embed_url STRING, conversion_type STRING, "
        "iframe_heatmap_url STRING, browser STRING, browser_version STRING, "
        "platform STRING, mobile BOOLEAN, thumbnail_url STRING, ingested_at STRING"
    ),
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
          "engagement": float(r["engagement"]),
          "engagement_data": [int(x) for x in (r["engagement_data"] or [])],
          "rewatch_data": [int(x) for x in (r.get("rewatch_data") or [])],
          "ingested_at": r["ingested_at"],
      })

    elif endpoint_name == "media_metadata":
      thumb = r.get("thumbnail") or {}
      folder = r.get("folder") if isinstance(r.get("folder"), dict) else {}
      transformed.append({
          "id": int(r["id"]),
          "hashed_id": r["hashed_id"],
          "name": r["name"],
          "type": r["type"],
          "archived": bool(r["archived"]),
          "duration": float(r["duration"]),
          "created": r["created"],
          "updated": r["updated"],
          "description": r.get("description") or "",
          "protected": str(r["protected"]) if r.get("protected") is not None else None,
          "status": str(r["status"]) if r.get("status") is not None else None,
          "thumbnail_url": str(thumb["url"]) if thumb.get("url") else None,
          "thumbnail_width": int(thumb["width"]) if thumb.get("width") is not None else None,
          "thumbnail_height": int(thumb["height"]) if thumb.get("height") is not None else None,
          "folder_id": int(folder["id"]) if folder.get("id") is not None else None,
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
          "lat": float(r["lat"]) if r.get("lat") is not None else None,
          "lon": float(r["lon"]) if r.get("lon") is not None else None,
          "org": r.get("org"),
          "email": r.get("email"),
          "percent_viewed": float(r["percent_viewed"]),
          "embed_url": r.get("embed_url"),
          "conversion_type": r.get("conversion_type"),
          "iframe_heatmap_url": str(r["iframe_heatmap_url"]),
          "browser": ua.get("browser"),
          "browser_version": ua.get("browser_version"),
          "platform": ua.get("platform"),
          "mobile": bool(ua["mobile"]) if ua.get("mobile") is not None else None,
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
