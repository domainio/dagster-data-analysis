import logging
from dagster import asset, Config, Output, MetadataValue
import pandas as pd
import requests
from io import StringIO
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FetchTaxiDataConfig(Config):
    data_url: str = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    nrows: int = 1000000  # Adjust as needed

@asset
def fetch_taxi_data(context, config: FetchTaxiDataConfig):
    logger.info(f"Starting to fetch data from {config.data_url}")
    start_time = time.time()
    try:
        response = requests.get(config.data_url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        downloaded = 0
        content = ""
        
        for data in response.iter_content(block_size):
            size = len(data)
            downloaded += size
            content += data.decode('utf-8')
            
            if total_size > 0:
                percent = downloaded / total_size * 100
                elapsed_time = time.time() - start_time
                download_speed = downloaded / elapsed_time / 1024 / 1024  # MB/s
                
                context.log.info(f"Downloaded {downloaded/1024/1024:.2f} MB of {total_size/1024/1024:.2f} MB ({percent:.2f}%)")
                context.log.info(f"Download speed: {download_speed:.2f} MB/s")
                
                context.log_event(
                    AssetObservation(
                        asset_key="fetch_taxi_data",
                        metadata={
                            "downloaded_mb": downloaded / 1024 / 1024,
                            "total_mb": total_size / 1024 / 1024,
                            "percent_complete": percent,
                            "download_speed_mb_per_s": download_speed
                        }
                    )
                )
            
            if downloaded % (block_size * 1000) == 0:  # Every ~8MB
                context.log.info(f"Downloaded {downloaded/1024/1024:.2f} MB")
        
        df = pd.read_csv(StringIO(content), nrows=config.nrows)
        logger.info(f"Successfully fetched {len(df)} rows of data")
        
        elapsed_time = time.time() - start_time
        metadata = {
            "row_count": len(df),
            "download_size_mb": downloaded/1024/1024,
            "total_size_mb": total_size/1024/1024,
            "download_time_seconds": elapsed_time,
            "average_download_speed_mb_per_s": (downloaded/1024/1024) / elapsed_time,
            "columns": MetadataValue.json(df.columns.tolist()),
        }
        
        return Output(df, metadata=metadata)
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in fetch_taxi_data: {e}")
        raise

@asset
def cleaned_taxi_data(fetch_taxi_data: pd.DataFrame):
    logger.info("Starting to clean taxi data")
    try:
        df = fetch_taxi_data[fetch_taxi_data['fare_amount'] >= 2.5]
        df = df[df['trip_duration'] <= 180]
        logger.info(f"Cleaned data has {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error in cleaned_taxi_data: {e}")
        raise

@asset
def analyze_taxi_data(cleaned_taxi_data: pd.DataFrame):
    logger.info("Starting to analyze taxi data")
    try:
        analysis = {
            "total_trips": len(cleaned_taxi_data),
            "average_fare": cleaned_taxi_data['fare_amount'].mean(),
            "average_duration": cleaned_taxi_data['trip_duration'].mean(),
        }
        logger.info("Analysis complete")
        return analysis
    except Exception as e:
        logger.error(f"Error in analyze_taxi_data: {e}")
        raise
