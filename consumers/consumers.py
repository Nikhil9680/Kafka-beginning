import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient

# ----------------- LOGGER SETUP -----------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ----------------- KAFKA CONFIG -----------------
KAFKA_BROKER = "localhost:19092"  # or "kafka:9092" inside Docker
GROUP_ID = "mongo-consumer-group"

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

consumer = Consumer(consumer_conf)

# This dynamically subscribe to all topics
# if we wanted to ,we  can also replace with ["racing_games", "sports_games", "arcade_games"]
topics = consumer.list_topics().topics.keys()
consumer.subscribe(list(topics))

logging.info(f"Subscribed to topics: {list(topics)}")

# ----------------- MONGODB CONFIG -----------------
mongo_user = "admin"
mongo_pass = "admin"
mongo_host = "localhost"
mongo_port = 27017


mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/?authSource=admin"
mongo_client = MongoClient(mongo_uri)
db = mongo_client["gaming_data"]

logging.info("Connected to MongoDB and Kafka.")

# ----------------- CONSUMER LOOP -----------------
try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        topic_name = msg.topic()
        collection = db[topic_name]
        value = json.loads(msg.value().decode("utf-8"))

        # Add Kafka metadata
        value["_kafka_metadata"] = {
            "topic": topic_name,
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": datetime.fromtimestamp(msg.timestamp()[1] / 1000).isoformat(),
        }

        collection.insert_one(value)

        logging.info(
            f"Inserted record into MongoDB | topic={topic_name} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )

except KeyboardInterrupt:
    logging.info("Consumer stopped manually.")

finally:
    consumer.close()
    mongo_client.close()
    logging.info("Closed Kafka and MongoDB connections.")
