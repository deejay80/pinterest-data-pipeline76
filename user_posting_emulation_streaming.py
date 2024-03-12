import requests
from time import sleep
import random
import json
from sqlalchemy import text, create_engine
from datetime import datetime

random.seed(100)

class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4", pool_size=5, max_overflow=10)
        return engine

def serialize_datetime(obj):
    """Serialize datetime object to string."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def send_to_kinesis(data, stream_name):
    records = []
    for row in data:
        # Convert datetime objects to string
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = serialize_datetime(value)
            elif isinstance(value, bytes):
                row[key] = value.decode('utf-8')
        records.append({"Data": json.dumps(row), "PartitionKey": str(random.randint(1, 1000))})
    
    payload = {"Records": records}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(f"https://kinesis.us-east-1.amazonaws.com/streams/{stream_name}", headers=headers, data=json.dumps(payload))
    
    if response.status_code == 200:
        print(f"Data sent to Kinesis stream {stream_name} successfully.")
    else:
        print(f"Failed to send data to Kinesis stream {stream_name}. Status code: {response.status_code}")

def fetch_random_row(connection, table):
    random_row = random.randint(0, 11000)
    query_string = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
    result = connection.execute(query_string)
    return [dict(row._mapping) for row in result]

def run_infinite_post_data_loop():
    connector = AWSDBConnector()
    
    while True:
        sleep(random.random() * 2)
        
        try:
            engine = connector.create_db_connector()
            with engine.connect() as connection:
                pin_result = fetch_random_row(connection, "pinterest_data")
                geo_result = fetch_random_row(connection, "geolocation_data")
                user_result = fetch_random_row(connection, "user_data")
                
                send_to_kinesis(pin_result, "streaming-12d5bb99b7ad-pin")
                send_to_kinesis(geo_result, "streaming-12d5bb99b7ad-geo")
                send_to_kinesis(user_result, "streaming-12d5bb99b7ad-user")
                
        except Exception as e:
            print(f"Error occurred: {e}")

if __name__ == "__main__":
    run_infinite_post_data_loop()
 