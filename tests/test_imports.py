import numpy
import pandas
import pyarrow

print(f"NumPy version: {numpy.__version__}")
print(f"Pandas version: {pandas.__version__}")
print(f"PyArrow version: {pyarrow.__version__}")

# Try to read a Parquet file
import pandas as pd
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
df = pd.read_parquet(url)
print(df.head())
