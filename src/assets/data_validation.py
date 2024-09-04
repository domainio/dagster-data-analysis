from pydantic import BaseModel, validator
from dagster import asset
import pandas as pd

class TaxiTrip(BaseModel):
    trip_duration: float
    fare_amount: float
    cost_per_minute: float

    @validator('trip_duration', 'fare_amount', 'cost_per_minute')
    def must_be_positive(cls, v):
        if v <= 0:
            raise ValueError('Must be positive')
        return v

@asset
def validated_taxi_data(cleaned_taxi_data: pd.DataFrame):
    """Validate cleaned taxi data"""
    validated_data = []
    for _, row in cleaned_taxi_data.iterrows():
        try:
            TaxiTrip(
                trip_duration=row['trip_duration'],
                fare_amount=row['fare_amount'],
                cost_per_minute=row['cost_per_minute']
            )
            validated_data.append(row)
        except ValueError as e:
            print(f"Validation error: {e}")
    
    return pd.DataFrame(validated_data)
