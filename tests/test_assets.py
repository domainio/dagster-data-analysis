import pytest
import pandas as pd
from src.assets.data_ingestion import nyc_taxi_data
from src.assets.data_transformation import cleaned_taxi_data
from src.assets.data_validation import validated_taxi_data

@pytest.fixture
def sample_taxi_data():
    return pd.DataFrame({
        'tpep_pickup_datetime': ['2023-01-01 00:00:00', '2023-01-01 01:00:00'],
        'tpep_dropoff_datetime': ['2023-01-01 00:30:00', '2023-01-01 02:00:00'],
        'fare_amount': [10.5, 20.0],
    })

def test_cleaned_taxi_data(sample_taxi_data):
    cleaned_data = cleaned_taxi_data(sample_taxi_data)
    assert 'trip_duration' in cleaned_data.columns
    assert 'cost_per_minute' in cleaned_data.columns
    assert all(cleaned_data['fare_amount'] >= 0)

def test_validated_taxi_data(sample_taxi_data):
    cleaned_data = cleaned_taxi_data(sample_taxi_data)
    validated_data = validated_taxi_data(cleaned_data)
    assert len(validated_data) == len(sample_taxi_data)
    assert all(validated_data['trip_duration'] > 0)
    assert all(validated_data['fare_amount'] > 0)
    assert all(validated_data['cost_per_minute'] > 0)

# Add more tests as needed
