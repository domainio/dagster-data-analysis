from pydantic import BaseModel
from dagster import asset
import requests

class WeatherData(BaseModel):
    temperature: float
    humidity: float
    wind_speed: float

@asset
def fetch_weather_data() -> WeatherData:
    response = requests.get('https://api.weather.com/current')
    return WeatherData(**response.json())