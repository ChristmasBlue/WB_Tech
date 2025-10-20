package parser

import (
	"bytes"
	"encoding/json"
	"log"
	"test_WB/models"
)

func ParseJsonOrder(r *bytes.Buffer) (*models.Order, error) {
	var order models.Order
	decoder := json.NewDecoder(r)

	//декодируем
	if err := decoder.Decode(&order); err != nil {
		log.Printf("Error decoding orderuid: %s.\n", order.OrderUID)
		return nil, err
	}

	//валидируем структуру
	if err := order.Validate(); err != nil {
		log.Printf("Validation failed for order %s: %v\n", order.OrderUID, err)
		return nil, err
	}

	log.Printf("Successfully parsed and validated order: %s", order.OrderUID)
	return &order, nil
}

func CreateJsonOrder(order *models.Order) (*bytes.Buffer, error) {
	//Валидируем перед созданием json
	if err := order.Validate(); err != nil {
		log.Printf("Validation failed before JSON creation: %v", err)
		return nil, err
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(order); err != nil {
		log.Printf("Error encoding order uid: %s.\n", order.OrderUID)
		return nil, err
	}
	return &buf, nil
}
