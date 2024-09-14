import asyncio
import websockets
import json
import logging
from kafka import KafkaProducer
# from airflow import DAG
# from airflow.operators.python import PythonOperator
from datetime import datetime

# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['broker:9092'],  # Adjust to your Kafka broker address
    # bootstrap_servers=['localhost:9092'],  # Adjust to your Kafka broker address
    max_block_ms=5000,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON data
)

async def fetch_data():
    url = "wss://ws.coinapi.io/v1/"
    api_key = "C75C9189-93B0-4130-A19F-30857A08DDD5"

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
                logging.error(f"Connection closed with error: {e}")
                break
            except Exception as e:
                logging.error(f"An error occurred: {e}")
                break

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 1, 1, 10, 00),
# }

def fetch_data_sync():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_data())

fetch_data_sync()

# with DAG('crypto_websocket_dag',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_websocket',
#         python_callable=fetch_data_sync
#     )

