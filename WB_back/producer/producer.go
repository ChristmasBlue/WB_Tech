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
		writer: &kafka.Writer{
			Addr:      kafka.TCP(brokers...),
			Topic:     topic,
			Balancer:  &kafka.Hash{},
			BatchSize: 1,
		},
	}
}

func (p *Producer) SendOrderProducer(msg *bytes.Buffer, key string) error {
	err := p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: msg.Bytes(),
			Time:  time.Now(),
		},
	)
	if err != nil {
		log.Printf("Error sending message: %v.\n", err)
		return err
	}
	log.Println("Message sent")
	log.Println("key: ", key, "message: ", msg.String())
	return nil
}

func (p *Producer) Close() {
	p.writer.Close()
}
