from pydantic import BaseModel, Field
from datetime import datetime

class TaxiRide(BaseModel):
    pickup_datetime: datetime
    dropoff_datetime: datetime
    passenger_count: int = Field(ge=1)
    trip_distance: float = Field(gt=0)
    fare_amount: float = Field(gt=0)
    tip_amount: float = Field(ge=0)
