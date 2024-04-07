import requests
import yaml
from time import sleep
import random
import json
from sqlalchemy import text, create_engine
from datetime import datetime

random.seed(100)

class AWSDBConnector:
    """Class to handle AWS RDS database connection."""
    
    def __init__(self, config_file):
        """
        Initialize AWSDBConnector with database credentials.

        Args:
            config_file (str): Path to the YAML configuration file.
        """
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        self.HOST = config['database']['host']
        self.USER = config['database']['user']
        self.PASSWORD = config['database']['password']
        self.DATABASE = config['database']['name']
        self.PORT = config['database']['port']
        
    def create_db_connector(self):
        """Create and return SQLAlchemy engine for database connection."""
        engine = create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4", pool_size=5, max_overflow=10)
        return engine

def serialize_datetime(obj):
    """
    Serialize datetime object to string.

    Args:
        obj (datetime): Datetime object to serialize.

    Returns:
        str: Serialized datetime string.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def send_to_kinesis(data, stream_name):
    """
    Send data to Kinesis stream.

    Args:
        data (list): List of dictionaries representing data to be sent.
        stream_name (str): Name of the Kinesis stream to send data to.
    """
    records = []
    partition_key = "my_partition_key"  # Fixed partition key

    for row in data:
        # Convert datetime objects to string
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = serialize_datetime(value)
            elif isinstance(value, bytes):
                row[key] = value.decode('utf-8')
        records.append({"Data": json.dumps(row), "PartitionKey": partition_key})
    
    payload = {"Records": records}
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.put(f"https://f1fxsgva0f.execute-api.us-east-1.amazonaws.com/prod", headers=headers, data=json.dumps(payload))
        # Check for HTTP status code
        response.raise_for_status()
        print(f"Data sent to Kinesis stream {stream_name} successfully.")
    except Exception as e:
        print(f"Failed to send data to Kinesis stream {stream_name}. Error: {e}")

def fetch_random_row(connection, table):
    """
    Fetch a random row from a database table.

    Args:
        connection: Database connection object.
        table (str): Name of the database table to fetch from.

    Returns:
        list: List containing a single dictionary representing the fetched row.
    """
    random_row = random.randint(0, 11000)
    query_string = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
    
    try:
        result = connection.execute(query_string)
        return [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"Error fetching random row from {table}: {e}")
        return []

def run_infinite_post_data_loop(config_file):
    """
    Run an infinite loop to periodically post data to Kinesis streams.

    Args:
        config_file (str): Path to the YAML configuration file.
    """
    connector = AWSDBConnector(config_file)
    
    stream_names = {
        "stream_name_for_pin": "pinterest_data",
        "stream_name_for_geo": "geolocation_data",
        "stream_name_for_user": "user_data"
    }
    
    while True:
        sleep(random.random() * 2)
        
        try:
            engine = connector.create_db_connector()
            with engine.connect() as connection:
                for stream_name, table in stream_names.items():
                    result = fetch_random_row(connection, table)
                    send_to_kinesis(result, stream_name)
                
        except Exception as e:
            print(f"Error occurred: {e}")

if __name__ == "__main__":
    config_file = "config.yaml"
    run_infinite_post_data_loop(config_file)
