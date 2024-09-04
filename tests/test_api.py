import pytest
from fastapi.testclient import TestClient
from src.api.main import app
from unittest.mock import patch, MagicMock
import pandas as pd

client = TestClient(app)

@pytest.fixture
def mock_db_connection():
    with patch('src.api.main.get_db_connection') as mock:
        yield mock

def test_get_average_fare_success(mock_db_connection):
    mock_conn = MagicMock()
    mock_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = [(20.5,)]
    
    response = client.get("/average_fare")
    assert response.status_code == 200
    assert response.json() == {"average_fare": 20.5}

def test_get_average_fare_no_data(mock_db_connection):
    mock_conn = MagicMock()
    mock_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = []
    
    response = client.get("/average_fare")
    assert response.status_code == 404
    assert response.json() == {"detail": "No data found"}

def test_get_trip_stats_success(mock_db_connection):
    mock_conn = MagicMock()
    mock_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = [(15.0, 30.0, 5.0, 25.0, 50.0, 10.0)]
    
    response = client.get("/trip_stats")
    assert response.status_code == 200
    assert response.json() == {
        "avg_duration": 15.0,
        "max_duration": 30.0,
        "min_duration": 5.0,
        "avg_fare": 25.0,
        "max_fare": 50.0,
        "min_fare": 10.0
    }

def test_get_trip_stats_no_data(mock_db_connection):
    mock_conn = MagicMock()
    mock_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = []
    
    response = client.get("/trip_stats")
    assert response.status_code == 404
    assert response.json() == {"detail": "No data found"}
