import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text, create_engine
from datetime import datetime
import yaml

random.seed(100)

class AWSDBConnector:
    """Class to handle AWS RDS database connection."""

    def __init__(self, credentials):
        """Initialize AWSDBConnector with database credentials."""
        self.credentials = credentials
        
    def create_db_connector(self):
        """Create and return SQLAlchemy engine for database connection."""
        engine = create_engine(
            f"mysql+pymysql://{self.credentials['user']}:{self.credentials['password']}@"
            f"{self.credentials['host']}:{self.credentials['port']}/{self.credentials['name']}?charset=utf8mb4",
            pool_size=5, max_overflow=10
        )
        return engine

def load_credentials(filename):
    """Load database credentials from YAML file."""
    try:
        with open(filename, 'r') as file:
            credentials = yaml.safe_load(file)['database']
        return credentials
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"Error: {e}")
        return None

def serialize_datetime(obj):
    """Serialize datetime object to string."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def send_to_kafka(data, topic, invoke_url):
    """Send data to Kafka topic."""
    records = [{key: serialize_datetime(value) if isinstance(value, datetime) else value for key, value in row.items()} for row in data]
    payload = json.dumps({"records": [{"value": row} for row in records]})
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.post(f"{invoke_url}/{topic}", headers=headers, data=payload)
    
    if response.status_code == 200:
        print(f"Data sent to Kafka topic '{topic}' successfully.")
    else:
        print(f"Failed to send data to Kafka topic '{topic}'. Status code: {response.status_code}")

def fetch_random_data(credentials, topic, invoke_url):
    """Fetch random data from MySQL tables and send it to Kafka."""
    random_row = random.randint(0, 11000)
    connector = AWSDBConnector(credentials)
    engine = connector.create_db_connector()

    with engine.connect() as connection:
        tables = ['pinterest_data', 'geolocation_data', 'user_data']
        for table in tables:
            query = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
            result = [dict(row._mapping) for row in connection.execute(query)]
            send_to_kafka(result, topic, invoke_url)

def run_infinite_loop(credentials_file, invoke_url):
    """Run an infinite loop to periodically fetch data and send it to Kafka."""
    credentials = load_credentials(credentials_file)
    if not credentials:
        return
    
    while True:
        sleep(random.randrange(0, 2))
        fetch_random_data(credentials, 'topic_name_for_pin', invoke_url)
        fetch_random_data(credentials, 'topic_name_for_geo', invoke_url)
        fetch_random_data(credentials, 'topic_name_for_user', invoke_url)

if __name__ == "__main__":
    credentials_file = "config.yaml"
    invoke_url = "https://f1fxsgva0f.execute-api.us-east-1.amazonaws.com/staging/topics"
    run_infinite_loop(credentials_file, invoke_url)
