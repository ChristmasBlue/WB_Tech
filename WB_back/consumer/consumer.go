package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// StartOrderConsumer обработчик запроса UID
func StartOrderGetConsumer(ctx context.Context, brokers []string, topic string, key string, groupID string, messageGetChan chan<- string) {
	go func() {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			MaxWait: 1 * time.Second,
		})
		defer reader.Close()

		log.Println("Starting order consumer...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Consumer stopping...")
				return
			default:
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.Printf("Error: %v", err)
					continue
				}
				if string(msg.Key) == key {
					messageGetChan <- string(msg.Value)
					log.Printf("Processed %s: partition %d, offset %d",
						key, msg.Partition, msg.Offset)
				}
			}
		}
	}()
}

// StartOrderAddConsumer обработчик json
func StartOrderAddConsumer(ctx context.Context, brokers []string, topic string, key string, groupID string, messageAddChan chan<- *bytes.Buffer) {
	go func() {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			MaxWait: 1 * time.Second,
		})
		defer reader.Close()
		log.Println("Starting order consumer...")
		for {
			select {
			case <-ctx.Done():
				log.Println("Consumer stopping...")
				return
			default:
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						log.Printf("INFO: Consumer stopping gracefully - context canceled for topic=%s group=%s\n",
							topic, groupID)
						return
					}
					log.Printf("Error: %v", err)
					continue
				}
				if string(msg.Key) == key {
					if json.Valid(msg.Value) {
						messageAddChan <- bytes.NewBuffer(msg.Value)
						log.Printf("Processed %s: partition %d, offset %d\n", key, msg.Partition, msg.Offset)
					} else {
						log.Printf("Invalid message rejected: partition %d, offset %d\n", msg.Partition, msg.Offset)
					}
				}
			}
		}
	}()
}
