import asyncio
import websockets
import json
import logging
from kafka import KafkaProducer
import threading

# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],  # Adjust to your Kafka broker address
    max_block_ms=5000,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON data
)

async def fetch_data():
    url = "wss://ws.coinapi.io/v1/"
    api_key = "C75C9189-93B0-4130-A19F-30857A08DDD5"

    while True:
        try:
            async with websockets.connect(url) as websocket:
                hello_message = {
                    "type": "hello",
                    "apikey": api_key,
                    "heartbeat": True,
                    "subscribe_data_type": ["quote"],
                    "subscribe_filter_asset_id": ["BTC/USD"]
                }
                await websocket.send(json.dumps(hello_message))

                while True:
                    try:
                        response = await websocket.recv()
                        data = json.loads(response)
                        # Send data to Kafka topic
                        producer.send('crypto-topic', value=data)
                        print(data)
                    except websockets.ConnectionClosedError as e:
                        logging.error(f"WebSocket connection closed: {e}")
                        break
                    except Exception as e:
                        logging.error(f"An error occurred while receiving data: {e}")
                        break
        except (websockets.ConnectionClosedError, OSError) as e:
            logging.error(f"WebSocket connection error: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        
        # Wait before reconnecting
        logging.info("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)

def run_fetch_data():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_data())

# Run fetch_data in a separate thread
fetch_data_thread = threading.Thread(target=run_fetch_data, daemon=True)
fetch_data_thread.start()

# Main program continues running here
try:
    while True:
        # Perform other tasks or wait
        pass
except KeyboardInterrupt:
    logging.info("Main program interrupted and will exit.")

