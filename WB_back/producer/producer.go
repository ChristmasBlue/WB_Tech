package producer

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  brokers,
			Topic:    topic,
			Balancer: &kafka.Hash{},
		}),
	}
}

func (p *Producer) SendOrderProducer(msg *bytes.Buffer, key string) error {
	data := make([]byte, msg.Len())
	copy(data, msg.Bytes())
	err := p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(data),
			Time:  time.Now(),
		},
	)
	if err != nil {
		log.Printf("Error sending message: %v", err)
		return err
	}
	log.Println("Message sent")
	return nil
}

func (p *Producer) Close() {
	p.writer.Close()
}
