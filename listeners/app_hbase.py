import os
import json
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types
import happybase

def write_to_hbase(row_key, data):
    """Write data to HBase."""
    # Connect to HBase
    connection = happybase.Connection('hbase')  # Replace 'hbase' with your HBase host if different

    # Define table name and column family
    table_name = 'stream_table'
    column_family = 'cf1'

    # Check if table exists before creating it
    try:
        tables = connection.tables()
        if table_name not in tables:
            connection.create_table(
                table_name,
                {column_family: dict()}
            )
    except Exception as e:
        # Log error but continue execution
        print(f"Table creation error (ignoring if table exists): {e}")

    # Get a reference to the table
    table = connection.table(table_name)

    # Write data to the table
    table.put(row_key, data)

    # Verify data was written
    row = table.row(row_key)
    print(f"Data written to HBase: {row}")

    # Close the connection
    connection.close()

def kafka_sink_example():
    """A Flink task which sinks a Kafka topic and writes the data to HBase."""
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

    # Define a function to process and write data to HBase
    def process_and_sink(value):
        # Convert the value to a dictionary
        print(f"Received: {value}")
        data = json.loads(value)
        
        # Use time_exchange as row_key
        row_key = data.get("time_exchange", "default_row_key")
        
        # Prepare data for HBase
        hbase_data = {
            'cf1:ask_price': str(data.get("ask_price", "")),  # Ensure data is string
            'cf1:ask_size': str(data.get("ask_size", "")),
            'cf1:bid_price': str(data.get("bid_price", "")),
            'cf1:bid_size': str(data.get("bid_size", "")),
            'cf1:symbol_id': data.get("symbol_id", ""),
            'cf1:sequence': str(data.get("sequence", "")),
            'cf1:type': data.get("type", "")
        }
        
        # Write data to HBase
        write_to_hbase(row_key, hbase_data)

    # Apply the map function to process each message
    ds.map(process_and_sink, output_type=Types.STRING())

    # Execute the Flink job
    env.execute("kafka_sink_example")

if __name__ == "__main__":
    kafka_sink_example()

