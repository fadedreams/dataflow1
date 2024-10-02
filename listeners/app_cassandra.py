
import os
import json
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types
from cassandra.cluster import Cluster


def write_to_cassandra(row_key, data):
    """Write data to Cassandra."""
    # Connect to Cassandra
    cluster = Cluster(['cassandra'])  # Hostname from your Docker Compose
    session = cluster.connect()

    # Define keyspace and table name
    keyspace = 'crypto_keyspace'  # Replace with your keyspace
    table_name = 'stream_table'  # Replace with your table name

    # Create keyspace if it doesn't exist
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)

    # Create table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
        row_key text PRIMARY KEY,
        ask_price text,
        ask_size text,
        bid_price text,
        bid_size text,
        symbol_id text,
        sequence text,
        type text
    );
    """
    session.execute(create_table_query)

    # Insert data into the table
    insert_query = f"""
    INSERT INTO {keyspace}.{table_name} (row_key, ask_price, ask_size, bid_price, bid_size, symbol_id, sequence, type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    session.execute(insert_query, (row_key, data.get('cf1:ask_price'), data.get('cf1:ask_size'), data.get('cf1:bid_price'),
                                   data.get('cf1:bid_size'), data.get('cf1:symbol_id'), data.get('cf1:sequence'), data.get('cf1:type')))

    print(f"Data written to Cassandra: {data}")

    # Close the session and cluster connection
    session.shutdown()
    cluster.shutdown()


def kafka_sink_example():
    """A Flink task which sinks a Kafka topic and writes the data to Cassandra."""
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Kafka SQL connector jar for integration
    env.add_jars("file:///jars/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # Define the Kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BROKER"])
        .set_topics(os.environ["KAFKA_TOPIC"])
        .set_group_id("flink_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Create a data stream from the Kafka source
    ds = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source"
    )

    # Define a function to process and write data to Cassandra
    def process_and_sink(value):
        # Convert the value to a dictionary
        print(f"Received: {value}")
        data = json.loads(value)

        # Use time_exchange as row_key
        row_key = data.get("time_exchange", "default_row_key")

        # Prepare data for Cassandra
        cassandra_data = {
            # Ensure data is string
            'cf1:ask_price': str(data.get("ask_price", "")),
            'cf1:ask_size': str(data.get("ask_size", "")),
            'cf1:bid_price': str(data.get("bid_price", "")),
            'cf1:bid_size': str(data.get("bid_size", "")),
            'cf1:symbol_id': data.get("symbol_id", ""),
            'cf1:sequence': str(data.get("sequence", "")),
            'cf1:type': data.get("type", "")
        }

        # Write data to Cassandra
        write_to_cassandra(row_key, cassandra_data)

    # Apply the map function to process each message
    ds.map(process_and_sink, output_type=Types.STRING())

    # Execute the Flink job
    env.execute("kafka_sink_example")


if __name__ == "__main__":
    kafka_sink_example()
