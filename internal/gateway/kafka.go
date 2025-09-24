package gateway

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	Writer *kafka.Writer
	Reader *kafka.Reader
}

func (k *KafkaClient) Ping() error {
	conn, err := kafka.DialLeader(context.Background(), "tcp", k.Reader.Config().Brokers[0], k.Reader.Config().Topic, 0)
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}

func NewKafkaClient(broker []string, topic string, groupID string) *KafkaClient {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        broker,
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: time.Second,
		MinBytes:       10e3,
		MaxBytes:       10e6,
	})
	return &KafkaClient{
		Writer: writer,
		Reader: reader,
	}
}

func (k *KafkaClient) Publish(ctx context.Context, key, value []byte) error {
	return k.Writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	})
}

func (k *KafkaClient) Consume(ctx context.Context, handler func(key, value []byte)) {
	go func() {
		for {
			m, err := k.Reader.ReadMessage(ctx)
			if err != nil {
				log.Println("Kafka read error:", err)
				continue
			}
			handler(m.Key, m.Value)
		}
	}()
}
