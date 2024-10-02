import os
import rethinkdb as r
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types

# RethinkDB connection parameters
RETHINKDB_HOST = 'rethinkdb'
RETHINKDB_PORT = 28015
RETHINKDB_DB = 'dbstream'
RETHINKDB_TABLE = 'tbstream'

def connect_rethinkdb():
    """Connect to RethinkDB and create the table if it doesn't exist."""
    conn = r.connect(host=RETHINKDB_HOST, port=RETHINKDB_PORT, db=RETHINKDB_DB)
    
    # Create the table if it doesn't exist
    try:
        r.db(RETHINKDB_DB).table_create(RETHINKDB_TABLE, primary_key='id').run(conn)
    except r.ReqlOpFailedError:
        print(f"Table {RETHINKDB_TABLE} already exists.")
    
    return conn

def process_and_sink(value, conn):
    """Process the Kafka stream and save the data to RethinkDB."""
    # Print the received value
    print(f"Received: {value}")
    
    # Document to be inserted into RethinkDB
    doc = {
        'id': f"msg_{os.urandom(4).hex()}",
        'message': value
    }

    # Save to RethinkDB
    try:
        r.db(RETHINKDB_DB).table(RETHINKDB_TABLE).insert(doc).run(conn)
        print(f"Document inserted into RethinkDB: {doc}")
    except Exception as e:
        print(f"Error inserting data into RethinkDB: {e}")

def kafka_sink_example():
    """A Flink task which sinks a Kafka topic and saves the data to RethinkDB."""
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

    # Create RethinkDB connection
    conn = connect_rethinkdb()

    # Map function to process the Kafka stream and insert into RethinkDB
    ds.map(lambda value: process_and_sink(value, conn), output_type=Types.STRING())

    # Execute the Flink job
    env.execute("kafka_sink_example")

if __name__ == "__main__":
    kafka_sink_example()

