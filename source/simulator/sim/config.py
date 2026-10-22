import os
from pathlib import Path

import yaml


def load_config() -> dict:
    path = Path(os.getenv("SIM_CONFIG", Path(__file__).parent.parent / "config.yaml"))
    cfg = yaml.safe_load(path.read_text())
    cfg.setdefault("identity_url", os.getenv("IDENTITY_URL", "http://identity:8300"))
    if os.getenv("SOURCE_API_URL"):
        cfg["source_api_url"] = os.environ["SOURCE_API_URL"]
    if os.getenv("SIM_SCALE"):
        cfg["scale"]["preset"] = os.environ["SIM_SCALE"]
    if os.getenv("SIM_SPEED"):
        cfg["clock"]["speed"] = float(os.environ["SIM_SPEED"])
    if os.getenv("SIM_AUTO_SEED"):
        cfg["seed"]["auto"] = os.environ["SIM_AUTO_SEED"].lower() in ("1", "true", "yes")
    if cfg["scale"]["preset"] not in cfg["scale"]["presets"]:
        raise ValueError(f"Unknown scale preset {cfg['scale']['preset']!r}")
    return cfg
