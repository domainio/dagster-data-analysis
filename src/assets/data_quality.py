from dagster import asset
from src.quality.data_quality import DataQualityMetrics
import pandas as pd

@asset
def data_quality_check(cleaned_taxi_data: pd.DataFrame) -> DataQualityMetrics:
    metrics = DataQualityMetrics(
        total_rows=len(cleaned_taxi_data),
        null_percentage=cleaned_taxi_data.isnull().mean().mean() * 100,
        negative_fares=sum(cleaned_taxi_data['fare_amount'] < 0)
    )
    return metrics
