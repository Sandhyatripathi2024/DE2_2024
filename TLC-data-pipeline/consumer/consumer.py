import redis
import psycopg2
import json
import time

# Connect to Redis
def connect_to_redis():
    while True:
        try:
            client = redis.Redis(host="redis", port=6379, db=0)
            client.ping()
            print("Connected to Redis.")
            return client
        except redis.ConnectionError:
            print("Redis connection failed. Retrying...")
            time.sleep(5)

# Connect to PostgreSQL
def connect_to_postgres():
    while True:
        try:
            conn = psycopg2.connect(
                host="postgres",
                dbname="mydatabase",
                user="user",
                password="password"
            )
            print("Connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError:
            print("PostgreSQL connection failed. Retrying...")
            time.sleep(5)

# Main Consumer Logic
def main():
    redis_client = connect_to_redis()
    db_conn = connect_to_postgres()
    cursor = db_conn.cursor()

    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_data (
        VendorID INT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance FLOAT,
        pickup_longitude FLOAT,
        pickup_latitude FLOAT,
        RatecodeID INT,
        store_and_fwd_flag TEXT,
        dropoff_longitude FLOAT,
        dropoff_latitude FLOAT,
        payment_type INT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT
    );
    """)
    db_conn.commit()
    print("Created table if not exists.")

    # Start consuming data
    print("Starting to consume data from Redis...")
    while True:
        message = redis_client.blpop("data_queue", timeout=10)  # Blocking pop
        if message:
            _, value = message
            row = json.loads(value)
            cursor.execute("""
            INSERT INTO raw_data (
                VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
                trip_distance,pickup_longitude,pickup_latitude,RatecodeID, store_and_fwd_flag,dropoff_longitude, dropoff_latitude, payment_type,
                fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);
            """, 
            (
                row.get("VendorID"),
                row.get("tpep_pickup_datetime"),
                row.get("tpep_dropoff_datetime"),
                row.get("passenger_count"),
                row.get("trip_distance"),
                row.get("pickup_longitude"),
                row.get("pickup_latitude"),
                row.get("RatecodeID"),
                row.get("store_and_fwd_flag"),
                row.get("dropoff_longitude"),
                row.get("dropoff_latitude"),
                row.get("payment_type"),
                row.get("fare_amount"),
                row.get("extra"),
                row.get("mta_tax"),
                row.get("tip_amount"),
                row.get("tolls_amount"),
                row.get("improvement_surcharge"),
                row.get("total_amount")
            ))
            db_conn.commit()
            print(f"Inserted: {row}")
        else:
            print("No more data found. Consumer is waiting...")

# Run the main consumer
if __name__ == "__main__":
    main()
