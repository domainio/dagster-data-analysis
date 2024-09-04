from pydantic import BaseModel, validator

class DataQualityMetrics(BaseModel):
    total_rows: int
    null_percentage: float
    negative_fares: int

    @validator('null_percentage')
    def check_null_percentage(cls, v):
        if v > 5:
            raise ValueError(f"Null percentage too high: {v}%")
        return v
