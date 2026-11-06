from pathlib import Path
from typing import Any
from scripts.models.config_models import (
  MediaConfig,
  Endpoints,
  EnvironmentConfig,
  AwsConfig
)
import yaml

DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

REQUIRED_MEDIA_SECTIONS = ("api_base", "media_ids", "endpoints", "results_per_page")

REQUIRED_ENV_SECTIONS = ("env", "aws")


def get_config(config_name: str, config_dir: Path | str | None = None):
  directory = Path(config_dir) if config_dir else DEFAULT_CONFIG_DIR
  config_path = directory / f"{config_name}.yaml"
  
  if not config_path.is_file():
    available = sorted(p.stem for p in directory.glob("*.yaml"))
    raise FileNotFoundError(
        f"no config for config {config_name!r} at {config_path}; available: {available}"
    )

  with config_path.open(encoding="utf-8") as handle:
      yaml_config: Any = yaml.safe_load(handle)

  if not isinstance(yaml_config, dict):
    raise ValueError(f"{config_path}: expected a YAML mapping")
  return yaml_config, config_path


def load_media_config(config_name: str = "media", config_dir: Path | str | None = None) -> MediaConfig:
  media_yaml, config_path = get_config(config_name, config_dir)

  missing = [s for s in REQUIRED_MEDIA_SECTIONS if s not in media_yaml]
  if missing:
    raise ValueError(f"{config_path}: missing section(s) {missing}")

  if not media_yaml["api_base"]:
    raise ValueError(f"{config_path} is missing api_base entry")

  endpoints: dict[str, Endpoints] = {}
  
  for k, v in (media_yaml.get("endpoints") or {}).items():
    if not isinstance(v, dict):
      raise ValueError(f"{config_path} is missing {k}")

    try:
      endpoints[k] = Endpoints(**v)
    except (TypeError, ValueError) as exc:
      raise ValueError(f"{config_path}: endpoint {k!r} has the following issue: {exc}") from exc

  try:
    return MediaConfig(
      api_base=media_yaml["api_base"],
      media_ids=media_yaml["media_ids"],
      endpoints=endpoints,
      results_per_page=media_yaml["results_per_page"]
    )
  except (KeyError, ValueError) as exc:
    raise ValueError(f"{config_path}: issue building MediaConfig: {exc}") from exc


def load_environment_config(env: str, config_dir: Path | str | None = None) -> EnvironmentConfig:
  env_config, config_path = get_config(env, config_dir)
  missing = [s for s in REQUIRED_ENV_SECTIONS if s not in env_config]
  
  if missing:
    raise ValueError(f"{config_path}: missing section(s) {missing}")

  aws_raw = env_config.get("aws")
  if not isinstance(aws_raw, dict):
    raise ValueError(f"{config_path}: 'aws' section must be a mapping")

  try:
    aws = AwsConfig(**aws_raw)
  except (TypeError, ValueError) as exc:
    raise ValueError(f"{config_path}: aws config for {env!r} has the following issue: {exc}") from exc

  try:
    return EnvironmentConfig(
      env=env_config["env"],
      aws=aws
    )
  except (KeyError, ValueError) as exc:
    raise ValueError(f"{config_path}: missing required key {exc}") from exc

    