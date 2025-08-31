package parser

import (
	"WB_Tech/models"
	"bytes"
	"encoding/json"
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
