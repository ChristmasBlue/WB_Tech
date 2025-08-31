package kafka

import (
	//"bytes"
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string, partition int) *Producer {
	return &Producer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  brokers,
			Topic:    topic,
			Balancer: &kafka.Hash{},
		}),
	}
}

func (p *Producer) SendOrderProducer(key string, msg []byte) error {
	err := p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: msg,
			Time:  time.Now(),
		},
	)
	if err != nil {
		log.Printf("Error sending message: %v", err)
		return err
	}
	log.Printf("Message sent")
	return nil
}

func (p *Producer) Close() {
	p.writer.Close()
}
