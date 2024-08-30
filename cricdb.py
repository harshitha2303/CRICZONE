import mysql.connector
import random
from kafka import KafkaProducer

# Connect to MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="23022003",
    database="cricket"  # Use your database name
)
cursor = mydb.cursor()

# Create the cricket_scores table if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS cricketscores (
                    PlayerID INT,
                    RunsScored INT,
                    WicketsTaken INT,
                    YearsOfExperience INT
                 )''')

# Generate and insert 100 rows of sample data
for _ in range(100):
    player_id = random.randint(1001, 1100)
    runs_scored = random.randint(0, 100)
    wickets_taken = random.randint(0, 10)
    years_of_experience = random.randint(0, 20)

    insert_query = "INSERT INTO cricketscores (PlayerID, RunsScored, WicketsTaken, YearsOfExperience) VALUES (%s, %s, %s, %s)"
    values = (player_id, runs_scored, wickets_taken, years_of_experience)
    cursor.execute(insert_query, values)

# Commit changes to the database
mydb.commit()

# Close database connection
mydb.close()

print("Successfully populated the cricketscores table with 100 rows of sample data.")

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Create Kafka topics
topics = ["runsscored", "iopwicketstaken", "topruns"]

# Produce data to Kafka topics
for _ in range(100):
    player_id = random.randint(1001, 1100)
    runs_scored = random.randint(0, 100)
    wickets_taken = random.randint(0, 10)
    years_of_experience = random.randint(0, 20)

    # Choose the topic based on data attributes
    if runs_scored > 50:
        topic = "topruns"
    elif wickets_taken >= 5:
        topic = "iopwicketstaken"
    else:
        topic = "runsscored"

    # Create message for Kafka topic
    message = f"{player_id},{runs_scored},{wickets_taken},{years_of_experience}"
    producer.send(topic, value=bytes(message, encoding='utf-8'))

# Close Kafka producer
producer.close()

print("Successfully produced data to Kafka topics.")

