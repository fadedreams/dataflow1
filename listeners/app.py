import os

from pyflink.common import WatermarkStrategy, Encoder
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, RollingPolicy, OutputFileConfig
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
import rethinkdb as r

def get_rethinkdb_connection():
    conn = r.connect(
        host=os.environ.get("RETHINKDB_HOST", "localhost"),
        port=int(os.environ.get("RETHINKDB_PORT", 28015))
    )
    return conn

def create_rethinkdb_table(conn, db_name, table_name):
    if db_name not in r.db_list().run(conn):
        r.db_create(db_name).run(conn)
    if table_name not in r.db(db_name).table_list().run(conn):
        r.db(db_name).table_create(table_name).run(conn)

def kafka_sink_example():
    """A Flink task which sinks a kafka topic to a file on disk

    We will read from a kafka topic and then perform a count() on
    the message to count the number of characters in the message.
    We will then save that count to the file on disk.
    """
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # the kafka/sql jar is used here as it's a fat jar and could avoid dependency issues
    env.add_jars("file:///jars/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # Define the new kafka source with our docker brokers/topics
    # This creates a source which will listen to our kafka broker
    # on the topic we created. It will read from the earliest offset
    # and just use a simple string schema for serialization (no JSON/Proto/etc)
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BROKER"])
        .set_topics(os.environ["KAFKA_TOPIC"])
        .set_group_id("flink_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Adding our kafka source to our environment
    ds = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source"
    )

    # Just count the length of the string. You could get way more complex
    # here
    # ds = ds.map(lambda a: len(a), output_type=Types.INT())
    ds = ds.map(lambda a: a, output_type=Types.STRING())

    output_path = os.path.join(os.environ.get("SINK_DIR", "/sink"), "sink.log")
    # This is the sink that we will write to
    sink = (
        FileSink.for_row_format(
            base_path=output_path, encoder=Encoder.simple_string_encoder()
        )
        .with_output_file_config(OutputFileConfig.builder().build())
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )

    # Writing the processed stream to the file
    ds.sink_to(sink=sink)

    # Define the RethinkDB sink
    def rethinkdb_sink(elements):
        conn = get_rethinkdb_connection()
        db_name = os.environ.get("RETHINKDB_DB", "test")
        table_name = os.environ.get("RETHINKDB_TABLE", "messages")
        create_rethinkdb_table(conn, db_name, table_name)
        for element in elements:
            r.db(db_name).table(table_name).insert({"message": element}).run(conn)
        conn.close()

    ds.add_sink(rethinkdb_sink)

    # Execute the job and submit the job
    env.execute("kafka_sink_example")


if __name__ == "__main__":
    kafka_sink_example()

