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
	"test_WB/config"
	"test_WB/consumer"
	"test_WB/parser"
	"test_WB/producer"
	"test_WB/service"

	_ "github.com/lib/pq"
)

func main() {
	//GetAddiction подлкючение переменных окружения
	configYaml := config.Load()

	ConnStr := "host=" + configYaml.DbHost + " user=" + configYaml.DbUser + " password=" + configYaml.DbPassword + " dbname=" + configYaml.DbName + " sslmode=" + configYaml.DbSslmode + " port=" + configYaml.DbPort
	if ConnStr == "" {
		log.Println("Error parameters DB empty.")
		return
	}

	brokers := configYaml.BrokersAddress
	if brokers == "" {
		log.Println("Error parameter brokers empty.")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	db, err := sql.Open(configYaml.DbType, ConnStr)
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
	consumer.StartOrderAddConsumer(ctx, []string{brokers}, configYaml.Topic, configYaml.KeyAddOrder, configYaml.GroupIdAdd, chanAddOrder)
	consumer.StartOrderGetConsumer(ctx, []string{brokers}, configYaml.Topic, configYaml.KeyGetOrder, configYaml.GroupIdGet, chanGetOrder)
	prod := producer.NewProducer([]string{brokers}, configYaml.Topic)
	defer prod.Close()
	orderCache := cache.NewCache(1000)

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
			orderCache.Add(orderUID, orderJson)
		}
	}

	var wg sync.WaitGroup
	//добавление данных в БД
	wg.Add(1)
	go func() {
		defer wg.Done()
		for mess := range chanAddOrder {

			log.Printf("ADD - Received message, size: %d bytes", mess.Len())

			messCopy := bytes.NewBuffer(mess.Bytes())
			order, err := parser.ParseJsonOrder(messCopy)
			if err != nil {
				log.Printf("Validation failed: %v", err)
				continue
			}

			log.Printf("ADD - Parsed order: %s", order.OrderUID)

			cacheBuffer := bytes.NewBuffer(mess.Bytes())

			log.Printf("ADD - Cache buffer size: %d bytes", cacheBuffer.Len())

			orderCache.Add(order.OrderUID, cacheBuffer)
			err = orderDb.AddDB(order)
			if err != nil {
				log.Printf("Error add data from DB: %v", err)
				continue
			}

			log.Printf("ADD - Order %s successfully processed", order.OrderUID)

		}
	}()
	//отправка запроса по UID
	wg.Add(1)
	go func() {
		defer wg.Done()
		for mess := range chanGetOrder {

			log.Printf("GET - Processing request for UID: '%s'", mess)

			buf, ok := orderCache.Get(mess)
			if !ok {
				log.Printf("Cache miss for UID: %s, querying DB", mess)
				order, err := orderDb.GetOrderUID(mess)
				if err != nil {
					log.Printf("Error get data from DB: %v", err)
					continue
				}
				if order != nil {

					log.Printf("GET - Found order in DB: %s", mess)

					jsonbuf, err := parser.CreateJsonOrder(order)
					if err != nil {
						log.Printf("Error create Json: %v", err)
						continue
					}

					log.Printf("GET - Created JSON, size: %d bytes", jsonbuf.Len())

					cacheBuf := bytes.NewBuffer(jsonbuf.Bytes())
					orderCache.Add(mess, cacheBuf)
					buf = jsonbuf
				} else {
					log.Printf("GET - Order not found in DB: %s", mess)
					continue
				}
			} else {
				log.Printf("GET - Cache hit for UID: %s, buffer size: %d bytes", mess, buf.Len())
			}
			err := prod.SendOrderProducer(buf, configYaml.KeySendOrder)
			if err != nil {
				log.Printf("Send error data in Kafka: %v", err)
				continue
			}
			log.Printf("GET - Response sent for UID: %s", mess)
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
