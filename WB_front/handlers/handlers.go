package handlers

import (
	"WB_Tech/kafka"
	"WB_Tech/models"
	"WB_Tech/parser"
	"bytes"
	"html/template"
	"io"
	"log"
	"net/http"
)

const (
	GetOrder = "Get"
)

func StartPage(templates *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			err := templates.ExecuteTemplate(w, "index.html", nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}
}

func OrderPage(prod *kafka.Producer, messageChan <-chan []byte, templates *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("=== ORDER PAGE HANDLER ===")
		log.Printf("Method: %s", r.Method)
		log.Printf("Content-Type: %s", r.Header.Get("Content-Type"))

		if r.Method == "POST" {
			// Читаем тело запроса для отладки
			bodyBytes, _ := io.ReadAll(r.Body)
			log.Printf("Raw body: %s", string(bodyBytes))
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // Восстанавливаем body

			// Парсим форму
			if err := r.ParseForm(); err != nil {
				log.Printf("ParseForm error: %v", err)
			}

			log.Printf("Form values: %v", r.Form)
			log.Printf("PostForm values: %v", r.PostForm)

			uid := r.FormValue("uid")
			log.Printf("Final UID: '%s'", uid)

			err := prod.SendOrderProducer(GetOrder, []byte(uid))
			if err != nil {
				log.Println("Error sending to Kafka:", err)
				order := &models.Order{}
				renderOrderTemplate(w, templates, order)
				return
			}
			mess := <-messageChan
			log.Printf("Back Mess: '%s'", string(mess))
			order := &models.Order{}
			if len(mess) != 0 {
				order, err = parser.ParseJsonOrder(bytes.NewBuffer(mess))
				if err != nil {
					log.Println("Error parsing JSON:", err)
				}
			}
			log.Printf("Parsed order: OrderUID=%s, TrackNumber=%s", order.OrderUID, order.TrackNumber)
			renderOrderTemplate(w, templates, order)
		} else {
			http.Redirect(w, r, "/", http.StatusSeeOther)
		}
	}
}

func renderOrderTemplate(w http.ResponseWriter, templates *template.Template, order *models.Order) {
	err := templates.ExecuteTemplate(w, "order.html", order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
