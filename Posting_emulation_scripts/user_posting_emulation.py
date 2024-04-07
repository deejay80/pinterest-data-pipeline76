import requests
from time import sleep
import random
import json
from multiprocessing import Process
import sqlalchemy
from sqlalchemy import text, create_engine
from datetime import datetime
import yaml

random.seed(100)

class AWSDBConnector:
    """Class to handle AWS RDS database connection."""

    def __init__(self, credentials):
        """
        Initialize AWSDBConnector with database credentials.

        Args:
            credentials (dict): Dictionary containing database credentials.
                Requires keys: 'host', 'user', 'password', 'name', 'port'.
        """
        self.HOST = credentials['host']
        self.USER = credentials['user']
        self.PASSWORD = credentials['password']
        self.DATABASE = credentials['name']
        self.PORT = credentials['port']
        
    def create_db_connector(self):
        """Create and return SQLAlchemy engine for database connection."""
        engine = create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4", pool_size=5, max_overflow=10)
        return engine

def load_credentials(filename):
    """
    Load database credentials from YAML file.

    Args:
        filename (str): Path to the YAML file containing database credentials.

    Returns:
        dict: Database credentials.
    """
    try:
        with open(filename, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials['database']
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error loading YAML file: {e}")
        return None

def serialize_datetime(obj):
    """Serialize datetime object to string."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def send_to_kafka(data, topic):
    """
    Send data to Kafka topic.

    Args:
        data (list): List of dictionaries representing data to be sent.
        topic (str): Kafka topic to send data to.
    """
    records = []
    for row in data:
        # Convert datetime objects to strings
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = serialize_datetime(value)
        records.append({"value": row})
    
    payload = json.dumps({"records": records})
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.post(invoke_url + f"/{topic}", headers=headers, data=payload)
    
    if response.status_code == 200:
        print(f"Data sent to Kafka topic {topic} successfully.")
    else:
        print(f"Failed to send data to Kafka topic {topic}. Status code: {response.status_code}")

def run_infinite_post_data_loop(credentials):
    """
    Run an infinite loop to periodically post data to Kafka topics.

    Args:
        credentials (dict): Database credentials.
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        connector = AWSDBConnector(credentials)
        engine = connector.create_db_connector()

        with engine.connect() as connection:
            # Query pin data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            pin_result = [dict(row._mapping) for row in pin_selected_row]

            # Query geolocation data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            geo_result = [dict(row._mapping) for row in geo_selected_row]

            # Query user data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            user_result = [dict(row._mapping) for row in user_selected_row]

        # Send data to Kafka topics
        send_to_kafka(pin_result, "topic_name_for_pin")
        send_to_kafka(geo_result, "topic_name_for_geo")
        send_to_kafka(user_result, "topic_name_for_user")

if __name__ == "__main__":
    credentials = load_credentials("config.yaml")
    invoke_url = "https://f1fxsgva0f.execute-api.us-east-1.amazonaws.com/staging/topics"
    run_infinite_post_data_loop(credentials)
