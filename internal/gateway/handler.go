package gateway

import (
	"context"
	"log"
)

func ConnectKafkaToWebsocket(ctx context.Context, hub *Hub, kafka *KafkaClient) {
	kafka.Consume(ctx, func(key, value []byte) {
		log.Println("Kafka event:", string(value))
		hub.Broadcast <- value
	})
}
