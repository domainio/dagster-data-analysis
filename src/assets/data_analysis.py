import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dagster import asset

@asset
def analyze_taxi_data(validated_taxi_data: pd.DataFrame):
    """Perform analysis on validated taxi data"""
    # Calculate average fare by hour
    validated_taxi_data['hour'] = validated_taxi_data['tpep_pickup_datetime'].dt.hour
    avg_fare_by_hour = validated_taxi_data.groupby('hour')['fare_amount'].mean().reset_index()
    
    # Create a line plot for average fare by hour
    fig1 = px.line(avg_fare_by_hour, x='hour', y='fare_amount', title='Average Fare by Hour')
    fig1.write_html("avg_fare_by_hour.html")
    
    # Calculate average trip duration by hour
    avg_duration_by_hour = validated_taxi_data.groupby('hour')['trip_duration'].mean().reset_index()
    
    # Create a bar plot for average trip duration by hour
    fig2 = px.bar(avg_duration_by_hour, x='hour', y='trip_duration', title='Average Trip Duration by Hour')
    fig2.write_html("avg_duration_by_hour.html")
    
    # Calculate correlation between fare amount and trip duration
    correlation = validated_taxi_data['fare_amount'].corr(validated_taxi_data['trip_duration'])
    
    # Create a scatter plot of fare amount vs trip duration
    fig3 = px.scatter(validated_taxi_data, x='trip_duration', y='fare_amount', 
                      title=f'Fare Amount vs Trip Duration (Correlation: {correlation:.2f})')
    fig3.write_html("fare_vs_duration_scatter.html")
    
    # Calculate daily statistics
    validated_taxi_data['date'] = validated_taxi_data['tpep_pickup_datetime'].dt.date
    daily_stats = validated_taxi_data.groupby('date').agg({
        'fare_amount': ['mean', 'min', 'max'],
        'trip_duration': ['mean', 'min', 'max'],
        'tpep_pickup_datetime': 'count'
    }).reset_index()
    daily_stats.columns = ['date', 'avg_fare', 'min_fare', 'max_fare', 'avg_duration', 'min_duration', 'max_duration', 'trip_count']
    
    # Create a multi-line plot for daily statistics
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=daily_stats['date'], y=daily_stats['avg_fare'], mode='lines', name='Avg Fare'))
    fig4.add_trace(go.Scatter(x=daily_stats['date'], y=daily_stats['avg_duration'], mode='lines', name='Avg Duration'))
    fig4.add_trace(go.Scatter(x=daily_stats['date'], y=daily_stats['trip_count'], mode='lines', name='Trip Count'))
    fig4.update_layout(title='Daily Statistics', xaxis_title='Date', yaxis_title='Value')
    fig4.write_html("daily_statistics.html")
    
    return "Analysis completed. Plots saved as HTML files."
