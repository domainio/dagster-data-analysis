import logging
from dagster import repository, load_assets_from_modules, define_asset_job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import src.assets as assets

all_assets_job = define_asset_job("all_assets_job", selection="*")

@repository
def my_repository():
    try:
        loaded_assets = load_assets_from_modules([assets])
        logger.info("Assets loaded successfully")
        return [
            loaded_assets,
            all_assets_job,
        ]
    except Exception as e:
        logger.error(f"Error in repository setup: {e}")
        raise
