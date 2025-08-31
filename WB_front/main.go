package main

import (
	"fmt"
	"net/http"

	"WB_Tech/handlers"
	"WB_Tech/kafka"
	"context"
	"html/template"
	"log"

	//"github.com/gorilla/sessions"

	//"sync"
	"os"
	"os/signal"
	"syscall"
)

const (
	groupID            = "consumer-group"
	topic              = "orders"
	partitionGetOrder  = 0 // партиция для отправки UID по которому нужно получить данные
	partitionSendOrder = 2 //партиция для получения обработанных данных на основе UID
	GetOrder           = "Get"
	AddOrder           = "Add"
	SendOrder          = "Send"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092" // значение по умолчанию для локальной разработки
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	messageChan := make(chan []byte, 10)
	kafka.StartOrderConsumer(ctx, []string{brokers}, topic, SendOrder, groupID, messageChan)
	prodGet := kafka.NewProducer([]string{brokers}, topic, partitionGetOrder)
	defer prodGet.Close()
	var err error
	var templates *template.Template
	templates, err = template.ParseGlob("html/*.html")
	if err != nil {
		log.Fatal("Error loading templates:", err)
	}

	http.HandleFunc("/", handlers.StartPage(templates))
	http.HandleFunc("/order", handlers.OrderPage(prodGet, messageChan, templates))
	log.Println("Сервер запущен на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

	// Ожидание сигнала завершения
	<-sigChan
	fmt.Println("\nОстановка сервиса...")

	// Graceful shutdown
	cancel()
	close(messageChan)

	fmt.Println("Сервис остановлен")
}
