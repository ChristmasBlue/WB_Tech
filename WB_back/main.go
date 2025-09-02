package main

import (
	"bytes"
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"test_WB/cache"
	"test_WB/consumer"
	"test_WB/parser"
	"test_WB/producer"
	"test_WB/service"

	_ "github.com/lib/pq"
)

const (
	groupIDGet = "get-consumer-group"
	groupIDAdd = "add-consumer-group"
	topic      = "orders"
	GetOrder   = "Get"  // ключ для получения UID по которому нужно произвести обработку
	AddOrder   = "Add"  // ключ для получения данных которые добавить в БД
	SendOrder  = "Send" // ключ для отправки обработанных данных на основе UID
)

func main() {
	ConnStr := os.Getenv("DB_CONNECTION_STRING")
	if ConnStr == "" {
		ConnStr = "host=localhost user=demo_user password=1124 dbname=demo_db sslmode=disable port=5432"
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092" // значение по умолчанию для локальной разработки
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	db, err := sql.Open("postgres", ConnStr)
	if err != nil {
		log.Printf("Error connect to DB: %v", err)
		return
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	err = db.Ping()
	if err != nil {
		log.Printf("Error check connection from DB: %v", err)
		return
	}

	orderDb := service.NewOrderDB(db)
	chanAddOrder := make(chan *bytes.Buffer, 10)
	chanGetOrder := make(chan string, 10)
	consumer.StartOrderAddConsumer(ctx, []string{brokers}, topic, AddOrder, groupIDAdd, chanAddOrder)
	consumer.StartOrderGetConsumer(ctx, []string{brokers}, topic, GetOrder, groupIDGet, chanGetOrder)
	prod := producer.NewProducer([]string{brokers}, topic)
	defer prod.Close()
	cache := cache.NewCaсhe()

	//заполняем кэш данными из БД за последнее время TimeLastRec
	OrdersUIDs, err := orderDb.GetUIDs()
	if err != nil && OrdersUIDs != nil {
		for _, orderUID := range OrdersUIDs {
			order, err := orderDb.GetOrderUID(orderUID)
			if err != nil {
				log.Printf("Error get order by uid: %v", err)
				continue
			}
			orderJson, err := parser.CreateJsonOrder(order)
			if err != nil {
				log.Printf("Error create json: %v", err)
				continue
			}
			cache.Add(orderUID, orderJson)
		}
	}

	var wg sync.WaitGroup
	//добавление данных в БД
	wg.Add(1)
	go func() {
		defer wg.Done()
		for mess := range chanAddOrder {
			order, err := parser.ParseJsonOrder(mess)
			if err != nil {
				log.Printf("Error parse Json: %v", err)
				continue
			}

			cache.Add(order.OrderUID, mess)
			err = orderDb.AddDB(order)
			if err != nil {
				log.Printf("Error add data from DB: %v", err)
				continue
			}
		}
	}()
	//отправка запроса по UID
	wg.Add(1)
	go func() {
		defer wg.Done()
		for mess := range chanGetOrder {
			buf, ok := cache.Get(mess)
			if !ok {
				order, err := orderDb.GetOrderUID(mess)
				if err != nil {
					log.Printf("Error get data from DB: %v", err)
					continue
				}
				if order != nil {
					jsonbuf, err := parser.CreateJsonOrder(order)
					if err != nil {
						log.Printf("Error create Json: %v", err)
						continue
					}
					buf = *jsonbuf
					cache.Add(string(mess), &buf)
				}
			}
			err = prod.SendOrderProducer(&buf, SendOrder)
			if err != nil {
				log.Printf("Send error data in Kafka: %v", err)
				continue
			}

		}
	}()
	log.Println("Service start. Press Ctrl+C for stop.")

	// Ожидание сигнала завершения
	<-sigChan
	log.Println("\nStopping service...")

	// Graceful shutdown
	cancel()
	close(chanAddOrder)
	close(chanGetOrder)
	wg.Wait()

	log.Println("Service stopped")
}
