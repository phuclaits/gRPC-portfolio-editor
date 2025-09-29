package kafka

import (
    "context"
    "log"

    "github.com/IBM/sarama"
)

type Producer struct {
    sarama.AsyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
    cfg := sarama.NewConfig()
    cfg.Producer.RequiredAcks = sarama.WaitForAll
    cfg.Producer.Return.Successes = true
    cfg.Producer.Retry.Max = 5

    p, err := sarama.NewAsyncProducer(brokers, cfg)
    if err != nil {
        return nil, err
    }

    prod := &Producer{AsyncProducer: p}

    // Log lỗi nền
    go func() {
        for err := range p.Errors() {
            log.Printf("Kafka error: %v", err)
        }
    }()

    return prod, nil
}

// Send gửi 1 message vào topic
func (p *Producer) Send(ctx context.Context, topic string, key, value []byte) {
    msg := &sarama.ProducerMessage{
        Topic: topic,
        Key:   sarama.ByteEncoder(key),
        Value: sarama.ByteEncoder(value),
    }
    select {
    case p.Input() <- msg:
        // gửi vào input channel
    case <-ctx.Done():
        log.Printf("Send aborted: %v", ctx.Err())
    }
}

// Close đóng producer
func (p *Producer) Close() error {
    return p.AsyncProducer.Close()
}
