from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
import pandas as pd
from src.utils.config import load_config
import logging
from contextlib import contextmanager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Load database configuration
db_config = load_config("database")
db_url = db_config["development"]["url"]
engine = create_engine(db_url)

@contextmanager
def get_db_connection():
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()

@app.get("/average_fare")
async def get_average_fare():
    try:
        with get_db_connection() as conn:
            query = "SELECT AVG(fare_amount) as avg_fare FROM taxi_trips"
            df = pd.read_sql(query, conn)
        
        if df.empty or pd.isna(df['avg_fare'][0]):
            logger.warning("No data found or average fare is null")
            raise HTTPException(status_code=404, detail="No data found")
        
        avg_fare = float(df['avg_fare'][0])
        logger.info(f"Average fare calculated: {avg_fare}")
        return {"average_fare": avg_fare}
    except Exception as e:
        logger.error(f"Error calculating average fare: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/trip_stats")
async def get_trip_stats():
    try:
        with get_db_connection() as conn:
            query = """
            SELECT 
                AVG(trip_duration) as avg_duration,
                MAX(trip_duration) as max_duration,
                MIN(trip_duration) as min_duration,
                AVG(fare_amount) as avg_fare,
                MAX(fare_amount) as max_fare,
                MIN(fare_amount) as min_fare
            FROM taxi_trips
            """
            df = pd.read_sql(query, conn)
        
        if df.empty:
            logger.warning("No data found for trip statistics")
            raise HTTPException(status_code=404, detail="No data found")
        
        stats = df.to_dict(orient='records')[0]
        logger.info(f"Trip statistics calculated: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Error calculating trip statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
