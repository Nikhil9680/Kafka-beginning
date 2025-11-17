from confluent_kafka import Producer
import requests, json, time, logging
from datetime import datetime

# -------------------------
# Logging setup
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# -------------------------
# Kafka setup
# -------------------------
p = Producer({'bootstrap.servers': 'localhost:19092'})


def delivery_report(err, msg):
    """Callback for each message delivery result."""
    if err:
        logging.error(f"Delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [partition {msg.partition()}]")


def fetch_and_publish(category, topic):
    url = f"https://www.freetogame.com/api/games?category={category}"
    logging.info(f"Fetching data from: {url}")

    response = requests.get(url)
    if response.status_code != 200:
        logging.warning(f"Failed to fetch data for '{category}': {response.status_code}")
        return

    try:
        data = response.json()
        if not isinstance(data, list):
            logging.warning(f"Unexpected data format for '{category}': {type(data)}")
            logging.debug(f"Response preview: {str(data)[:200]}")
            return

        for game in data[:10]:  # limit to 10 games
            payload = json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                "category": category,
                **game
            })
            p.produce(
                topic,
                key=str(game.get("id", "")),
                value=payload,
                callback=delivery_report
            )

        p.flush()
        logging.info(f"Published {min(len(data), 10)} messages to topic '{topic}'")

    except Exception as e:
        logging.exception(f"Error while processing category '{category}': {e}")


if __name__ == "__main__":
    logging.info("Kafka Producer started.")
    while True:
        for cat in ["racing", "shooter", "strategy"]:
            fetch_and_publish(cat, f"{cat}_games")

        logging.info("Sleeping for 5 minutes before next cycle...\n")
        time.sleep(300)
