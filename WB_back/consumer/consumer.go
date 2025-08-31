package consumer

import (
	"bytes"
	"context"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// StartOrderConsumer обработчик запроса UID
func StartOrderGetConsumer(ctx context.Context, brokers []string, topic string, key string, groupID string, messageGetChan chan<- string) {
	go func() {
		for {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:        brokers,
				Topic:          topic,
				GroupID:        groupID,
				MaxWait:        1 * time.Second,
				CommitInterval: 1 * time.Second,
			})
			defer reader.Close()
			reader.SetOffsetAt(context.Background(), time.Now())

			log.Println("Starting order consumer...")
			for {
				select {
				case <-ctx.Done():
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
		}
	}()
}

// StartOrderAddConsumer обработчик json
func StartOrderAddConsumer(ctx context.Context, brokers []string, topic string, key string, groupID string, messageAddChan chan<- *bytes.Buffer) {
	go func() {
		for {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:        brokers,
				Topic:          topic,
				GroupID:        groupID,
				MaxWait:        1 * time.Second,
				CommitInterval: 1 * time.Second,
			})
			defer reader.Close()
			reader.SetOffsetAt(context.Background(), time.Now())
			log.Println("Starting order consumer...")
			for {
				select {
				case <-ctx.Done():
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
						messageAddChan <- bytes.NewBuffer(msg.Value)
						log.Printf("Processed %s: partition %d, offset %d",
							key, msg.Partition, msg.Offset)
					}
				}
			}
		}
	}()
}
