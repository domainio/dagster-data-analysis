from sqlalchemy import create_engine
from dagster import asset
from src.utils.config import load_config

@asset
def store_taxi_data(cleaned_taxi_data):
    """Store cleaned taxi data in the database"""
    db_config = load_config("database")
    db_url = db_config["development"]["url"]  # Use development for now
    
    engine = create_engine(db_url)
    cleaned_taxi_data.to_sql("taxi_trips", engine, if_exists="replace", index=False)
    
    return f"Data stored in {db_url}"

# You can test this function by running it directly
if __name__ == "__main__":
    import pandas as pd
    
    # Create a sample DataFrame for testing
    sample_data = pd.DataFrame({
        "trip_id": range(1, 6),
        "fare_amount": [10.5, 15.0, 20.5, 12.0, 18.5]
    })
    
    result = store_taxi_data(sample_data)
    print(result)
