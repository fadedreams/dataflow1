import os
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types
import rethinkdb as r

# RethinkDB connection parameters
RETHINKDB_HOST = os.environ['RETHINKDB_HOST']
RETHINKDB_PORT = int(os.environ['RETHINKDB_PORT'])
RETHINKDB_DB = os.environ['RETHINKDB_DB']
RETHINKDB_TABLE = os.environ['RETHINKDB_TABLE']



def kafka_sink_example():
    """A Flink task which sinks a Kafka topic and prints the data."""
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

    # Define a function to process and sink the data
    def process_and_sink(value):
        # Convert to JSON and print
        print(f"Received: {value}")
        
        # Open a new RethinkDB connection for each message
        rdb = r.RethinkDB()
        try:
            conn = rdb.connect(host=RETHINKDB_HOST, port=RETHINKDB_PORT, db=RETHINKDB_DB)
            rdb.db(RETHINKDB_DB).table(RETHINKDB_TABLE).insert({"message": value}).run(conn)
        except Exception as e:
            print(f"Error inserting data into RethinkDB: {e}")
        finally:
            conn.close()  # Always close the connection after the operation

    # Apply the map function to process each message
    ds.map(process_and_sink, output_type=Types.STRING())

    # Use Flink's print sink to output data to the console
    ds.print()

    # Execute the Flink job
    env.execute("kafka_sink_example")

if __name__ == "__main__":
    kafka_sink_example()

