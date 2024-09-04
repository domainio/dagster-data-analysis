import click
from dagster import DagsterInstance
from src.jobs.etl_pipeline import nyc_taxi_etl

@click.group()
def cli():
    """NYC Taxi Data Analysis CLI"""
    pass

@cli.command()
def run_pipeline():
    """Run the NYC Taxi ETL pipeline"""
    instance = DagsterInstance.get()
    result = nyc_taxi_etl.execute_in_process(instance=instance)
    
    if result.success:
        click.echo("Pipeline executed successfully!")
    else:
        click.echo("Pipeline execution failed.")
        for event in result.events_for_node("store_taxi_data"):
            if event.event_type_value == "STEP_FAILURE":
                click.echo(f"Error: {event.event_specific_data.error}")

@cli.command()
def show_average_fare():
    """Show the average fare from the database"""
    from sqlalchemy import create_engine, text
    from src.utils.config import load_config

    db_config = load_config("database")
    db_url = db_config["development"]["url"]
    engine = create_engine(db_url)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT AVG(fare_amount) as avg_fare FROM taxi_trips"))
        avg_fare = result.fetchone()[0]
        click.echo(f"The average fare is: ${avg_fare:.2f}")

if __name__ == "__main__":
    cli()
