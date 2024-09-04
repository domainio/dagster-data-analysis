# Data Analysis with Dagster

## Project Description

This project uses Dagster to create a data pipeline that fetches, cleans, and analyzes NYC taxi trip data. It demonstrates how to build a simple ETL (Extract, Transform, Load) process using Dagster's asset-based paradigm.

## Features

- Fetch NYC taxi trip data from a public dataset
- Clean and filter the data based on fare amount and trip duration
- Perform basic analysis on the cleaned data
- Utilize Dagster for workflow management and data orchestration

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/nyc-taxi-data-analysis.git
   cd nyc-taxi-data-analysis
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Usage

1. Start the Dagster UI:
   ```
   dagster dev
   ```

2. Open your web browser and navigate to `http://localhost:3000`

3. In the Dagster UI, navigate to the "Launchpad" tab

4. Select the `all_assets_job` and click "Launch Run"

5. Monitor the progress of your job in the Dagster UI

## Project Structure

```
nyc-taxi-data-analysis/
├── src/
│   ├── assets/
│   │   ├── __init__.py
│   │   └── data_ingestion.py
│   └── repository.py
├── .gitignore
├── README.md
└── requirements.txt
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- NYC Taxi & Limousine Commission for providing the dataset
- Dagster team for their excellent
