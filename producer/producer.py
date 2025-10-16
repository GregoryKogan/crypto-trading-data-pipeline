import websocket
import json
import os
import logging
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

EXCHANGE_WS_URL = os.getenv("EXCHANGE_WS_URL", "wss://stream.binance.com:9443/ws")
CRYPTO_PAIR = os.getenv("CRYPTO_PAIR", "btcusdt")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_trades")


def create_kafka_producer(retries=5, delay=5) -> KafkaProducer | None:
    """
    Creates a KafkaProducer instance with retry logic for broker availability.
    Args:
        retries: Number of retries to connect to the Kafka broker.
        delay: Delay in seconds between retries.
    """
    for retry in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logging.info("Successfully connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            logging.error(
                f"Kafka broker not available. Retrying in {delay} seconds... ({retry+1}/{retries})"
            )
            time.sleep(delay)
    logging.critical(
        "Could not connect to Kafka broker after several retries. Exiting."
    )
    return None


kafka_producer = create_kafka_producer()


def on_message(ws: websocket.WebSocketApp, message: str) -> None:
    """
    Callback function to handle incoming messages from the WebSocket.
    """
    if not kafka_producer:
        logging.error("Kafka producer is not available. Cannot process message.")
        return

    try:
        raw_data = json.loads(message)

        if raw_data.get("e") == "trade":
            formatted_trade = {
                "trade_id": raw_data["t"],
                "symbol": raw_data["s"],
                "price": raw_data["p"],
                "quantity": raw_data["q"],
                "trade_time": raw_data["T"],
                "is_buyer_maker": raw_data["m"],
            }

            kafka_producer.send(KAFKA_TOPIC, value=formatted_trade)
            logging.info(f"Published trade to Kafka: {formatted_trade['trade_id']}")
        else:
            logging.info(f"Received non-trade message: {raw_data}")

    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from message: {message}")
    except Exception as e:
        logging.error(f"An error occurred while processing message: {e}")


def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
    logging.error(f"WebSocket Error: {error}")


def on_close(
    ws: websocket.WebSocketApp, close_status_code: int, close_msg: str
) -> None:
    """
    Callback function for when the WebSocket connection is closed.
    Args:
        ws: WebSocketApp instance.
        close_status_code: Status code of the close event.
        close_msg: Message of the close event.
    """
    logging.warning(
        f"WebSocket connection closed. Status: {close_status_code}, Msg: {close_msg}. Attempting to reconnect..."
    )


def on_open(ws: websocket.WebSocketApp) -> None:
    """
    Callback function for when the WebSocket connection is opened.
    Sends the subscription message.
    """
    subscription_message = {
        "method": "SUBSCRIBE",
        "params": [f"{CRYPTO_PAIR}@trade"],
        "id": 1,
    }
    ws.send(json.dumps(subscription_message))
    logging.info(f"Subscribed to {CRYPTO_PAIR} trade stream.")


def run_producer(retry_delay: int = 5, max_delay: int = 60) -> None:
    """
    Runs the producer with an exponential backoff retry loop.
    Args:
        retry_delay: Initial delay in seconds.
        max_delay: Maximum delay in seconds.
    """
    if not kafka_producer:
        return

    ws_url = f"{EXCHANGE_WS_URL}/{CRYPTO_PAIR}@trade"
    logging.info(f"Connecting to WebSocket URL: {ws_url}")

    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever()
        except Exception as e:
            logging.error(f"WebSocketApp run_forever failed with error: {e}")

        logging.info(f"Reconnecting in {retry_delay} seconds...")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_delay)


if __name__ == "__main__":
    run_producer()
