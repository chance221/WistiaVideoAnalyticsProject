from pathlib import Path, PurePosixPath
from typing import Any
import zipfile
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


def _read_yaml_from_wheel_or_zip(
    directory: Path,
    config_name: str,
) -> str | None:
  """If `directory` is inside a `.whl` or `.zip` archive (e.g. AWS Glue `--extra-py-files`
  where `directory` is `/tmp/.../pkg.whl/config`), reads `<directory.name>/<config_name>.yaml`
  directly from the zip archive."""
  archive_path = directory.parent
  if not archive_path.is_file() or not zipfile.is_zipfile(archive_path):
    return None

  prefix = f"{directory.name}/"
  target_entry = f"{prefix}{config_name}.yaml"
  with zipfile.ZipFile(archive_path, "r") as zf:
    names = zf.namelist()
    if target_entry in names:
      return zf.read(target_entry).decode("utf-8")

    available = sorted(
        PurePosixPath(n).stem
        for n in names
        if n.startswith(prefix) and n.endswith(".yaml")
    )
    raise FileNotFoundError(
        f"no config for config {config_name!r} at {directory / f'{config_name}.yaml'}; available: {available}"
    )


def get_config(config_name: str, config_dir: Path | str | None = None):
  directory = Path(config_dir) if config_dir else DEFAULT_CONFIG_DIR
  config_path = directory / f"{config_name}.yaml"

  if config_path.is_file():
    raw_yaml_text = config_path.read_text(encoding="utf-8")
  else:
    zip_yaml_text = _read_yaml_from_wheel_or_zip(directory, config_name)
    if zip_yaml_text is not None:
      raw_yaml_text = zip_yaml_text
    else:
      available = sorted(p.stem for p in directory.glob("*.yaml"))
      raise FileNotFoundError(
          f"no config for config {config_name!r} at {config_path}; available: {available}"
      )

  yaml_config: Any = yaml.safe_load(raw_yaml_text)

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

    