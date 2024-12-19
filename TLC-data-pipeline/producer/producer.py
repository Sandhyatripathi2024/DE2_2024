import redis
import pandas as pd
import time
import json

# Connect to Redis with retries
for i in range(5):
    try:
        redis_client = redis.Redis(host="redis", port=6379, db=0)
        redis_client.ping()  # Test connection
        print("Connected to Redis.")
        break
    except redis.ConnectionError:
        print(f"Redis connection failed. Retrying... ({i+1}/5)")
        time.sleep(5)
else:
    raise Exception("Failed to connect to Redis after 5 attempts.")

# Read the CSV
try:
    data = pd.read_csv("TLC_data_pipeline.csv")
except FileNotFoundError:
    print("TLC_data_pipeline.csv file not found.")
    raise

# Push data row by row into the Redis queue
for _, row in data.iterrows():
    message = row.to_json()
    redis_client.rpush("data_queue", message)
    print(f"Produced: {message}")
    time.sleep(0.1)  # Simulate delay

print("Producer finished pushing data.")
