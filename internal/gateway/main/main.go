package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"

	gw "github.com/phuclaits/gRPC-portfolio-editor/internal/gateway"
)

func main() {
	ctx := context.Background()

	brokers := []string{"127.0.0.1:9092"}
	topic := "portfolio-updates"
	groupID := "gateway-group"

	kafkaClient := gw.NewKafkaClient(brokers, topic, groupID)

	err := kafkaClient.Ping()
	if err != nil {
		log.Fatalf(">>>>>>>>>>>>>>>>Không kết nối được Kafka: %v <<<<<<<<<<<", err)
	} else {
		log.Println("================ Kết nối Kafka thành công ================")
	}

	redisClient := gw.NewRedisClient("127.0.0.1:6379", "", 0)
	if err := redisClient.Ping(ctx); err != nil {
		log.Fatalf(">>>>>>>>>>>>>>>> Không kết nối được Redis: %v <<<<<<<<<<<", err)
	} else {
		log.Println("================ Kết nối Redis thành công ================")
	}

	hub := gw.NewHub()
	go hub.Run()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	
	gw.ConnectKafkaToWebsocket(ctx, hub, kafkaClient)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		gw.ServeWs(hub, w, r)
	})

	go func() {
		log.Println("Gateway WebSocket listening on :8080/ws")
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	// Graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Println("Shutting down gateway...")
}
