from dagster import asset
import pandas as pd

@asset
def cleaned_taxi_data(fetch_taxi_data: pd.DataFrame):
    print(f"Cleaning {len(fetch_taxi_data)} rows of data")  # Debug print
    cleaned_data = fetch_taxi_data.copy()  # Replace with actual cleaning logic
    print(f"Cleaned data has {len(cleaned_data)} rows")  # Debug print
    return cleaned_data

@asset
def analyze_taxi_data(cleaned_taxi_data: pd.DataFrame):
    print(f"Analyzing {len(cleaned_taxi_data)} rows of data")  # Debug print
    analysis_result = cleaned_taxi_data.describe()
    print("Analysis complete")  # Debug print
    return analysis_result
