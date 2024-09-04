from dagster import job
from src.assets.data_ingestion import fetch_taxi_data
from src.assets.data_transformation import cleaned_taxi_data, analyze_taxi_data
from src.assets.data_quality import data_quality_check
from src.integrations.weather_api import fetch_weather_data
from src.resources.database import database_resource

@job(resource_defs={"database": database_resource})
def nyc_taxi_etl():
    weather_data = fetch_weather_data()
    raw_data = fetch_taxi_data()
    cleaned_data = cleaned_taxi_data(raw_data)
    data_quality_check(cleaned_data)
    analyze_taxi_data(cleaned_data)
