package parser

import (
	"bytes"
	"encoding/json"
	"test_WB/models"
)

func ParseJsonOrder(r *bytes.Buffer) (*models.Order, error) {
	var order models.Order
	decoder := json.NewDecoder(r)
	err := decoder.Decode(&order)
	if err != nil {
		return nil, err
	}
	return &order, nil
}

func CreateJsonOrder(order *models.Order) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(order)
	if err != nil {
		return nil, err
	}
	return &buf, nil
}
