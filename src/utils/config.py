import yaml
from pathlib import Path

def load_config(config_name):
    config_path = Path(__file__).parent.parent.parent / "config" / f"{config_name}.yaml"
    with open(config_path, "r") as file:
        return yaml.safe_load(file)
