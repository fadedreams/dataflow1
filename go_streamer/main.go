package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/websocket"
)

const (
	kafkaBroker = "localhost:9092" // Adjust to your Kafka broker address
	kafkaTopic  = "crypto_price"
	wsURL       = "wss://ws.coinapi.io/v1/"
	apiKey      = "C75C9189-93B0-4130-A19F-30857A08DDD5"
)

var producer *kafka.Producer

func initKafkaProducer() {
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %s", err)
	}
}

func fetchData() {
	defer producer.Close()

	// Set up WebSocket connection
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatalf("Failed to connect to WebSocket: %s", err)
	}
	defer ws.Close()

	// Send hello message
	helloMessage := map[string]interface{}{
		"type":                      "hello",
		"apikey":                    apiKey,
		"heartbeat":                 true,
		"subscribe_data_type":       []string{"quote"},
		"subscribe_filter_asset_id": []string{"BTC/USD"},
	}
	helloMessageBytes, _ := json.Marshal(helloMessage)
	err = ws.WriteMessage(websocket.TextMessage, helloMessageBytes)
	if err != nil {
		log.Fatalf("Failed to send hello message: %s", err)
	}

	// Create a pointer to kafkaTopic
	topic := kafkaTopic

	// Receive and process messages
	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			log.Printf("Error reading message: %s", err)
			break
		}

		var data map[string]interface{}
		err = json.Unmarshal(message, &data)
		if err != nil {
			log.Printf("Error unmarshalling message: %s", err)
			continue
		}

		// Send data to Kafka topic
		dataBytes, _ := json.Marshal(data)
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          dataBytes,
		}, nil)
		if err != nil {
			log.Printf("Failed to produce message to Kafka: %s", err)
		}

		fmt.Println(data)
	}
}

func main() {
	initKafkaProducer()
	fetchData()
}

