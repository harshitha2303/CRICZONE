import random
import mysql.connector
from kafka import KafkaProducer

# Connect to MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="23022003",
    database="cricket"  # Use your database name
)
cursor = mydb.cursor()

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Function to fetch data from MySQL and publish to Kafka
def publish_to_kafka(topic, data):
    message = bytes(data, encoding='utf-8')
    producer.send(topic, value=message)
    print(f"Published to topic '{topic}': {data}")

# Continuously fetch data from MySQL and publish to Kafka topics
while True:
    try:
        # Fetch random data from MySQL cricket table
        cursor.execute("SELECT * FROM cricketscores ORDER BY RAND() LIMIT 1")
        row = cursor.fetchone()

        if row:
            player_id, runs_scored, wickets_taken, years_of_experience = row
            # Generate messages for each Kafka topic
            top_runs_message = f"{player_id},{runs_scored},{wickets_taken},{years_of_experience}"
            top_wickets_message = f"{player_id},{wickets_taken},{runs_scored},{years_of_experience}"
            scores_message = f"{player_id},{runs_scored},{wickets_taken},{years_of_experience}"

            # Publish to Kafka topics
            publish_to_kafka('topruns', top_runs_message)
            publish_to_kafka('topwickets', top_wickets_message)
            publish_to_kafka('scores', scores_message)

    except Exception as e:
        print(e)

# Close Kafka producer and MySQL connection
producer.close()
mydb.close()

