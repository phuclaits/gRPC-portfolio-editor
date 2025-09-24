package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"

	gw "github.com/phuclaits/gRPC-portfolio-editor/gateway"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "portfolio-updates"
	groupID := "gateway-group"

	kafkaClient := gw.NewKafkaClient(brokers, topic, groupID)

	hub := gw.NewHub()
	go hub.Run()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Nhận từ Kafka → push WS
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
