from dagster import define_asset_job

nyc_taxi_etl = define_asset_job(
    "nyc_taxi_etl",
    selection="*",
    config={
        "ops": {
            "nyc_taxi_data": {
                "config": {
                    "data_url": "https://your-data-url.com/taxi_data.csv"
                }
            }
        }
    }
)
