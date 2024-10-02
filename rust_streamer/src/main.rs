use futures_util::{SinkExt, StreamExt};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde_json::Value;
use std::error::Error;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const KAFKA_BROKER: &str = "localhost:9092"; // Adjust to your Kafka broker address
const KAFKA_TOPIC: &str = "crypto_price";
const WS_URL: &str = "wss://ws.coinapi.io/v1/";
const API_KEY: &str = "C75C9189-93B0-4130-A19F-30857A08DDD5";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let producer = init_kafka_producer()?;
    fetch_data(producer).await?;
    Ok(())
}

fn init_kafka_producer() -> Result<FutureProducer, Box<dyn Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKER)
        .create()?;
    Ok(producer)
}

async fn fetch_data(producer: FutureProducer) -> Result<(), Box<dyn Error>> {
    let (ws_stream, _) = connect_async(WS_URL).await?;
    let (mut write, read) = ws_stream.split();

    // Send hello message
    let hello_message = serde_json::json!({
        "type": "hello",
        "apikey": API_KEY,
        "heartbeat": true,
        "subscribe_data_type": ["quote"],
        "subscribe_filter_asset_id": ["BTC/USD"],
    });
    write.send(Message::Text(hello_message.to_string())).await?;

    let mut read = read.map(|message| message.unwrap().into_text().unwrap());

    while let Some(message) = read.next().await {
        let data: Value = serde_json::from_str(&message)?;

        // Send data to Kafka topic
        let data_bytes = serde_json::to_vec(&data)?;
        match producer
            .send(
                FutureRecord::to(KAFKA_TOPIC).payload(&data_bytes).key(""),
                tokio::time::Duration::from_secs(0),
            )
            .await
        {
            Ok(_) => println!("Message sent successfully"),
            Err((err, _)) => eprintln!("Error sending message: {:?}", err),
        }

        println!("{}", data);
    }

    Ok(())
}
