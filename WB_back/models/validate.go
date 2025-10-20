package models

import (
	"time"
)

// ValidationError кастомная ошибка валидации
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return e.Field + ": " + e.Message
}

func (o *Order) Validate() error {
	if o.OrderUID == "" {
		return ValidationError{
			Field:   "order_uid",
			Message: "OrderUID cannot be empty",
		}
	}

	if o.TrackNumber == "" {
		return ValidationError{
			Field:   "track_number",
			Message: "TrackNumber cannot be empty",
		}
	}

	if o.DateCreated == nil || o.DateCreated.IsZero() {
		return ValidationError{
			Field:   "date_created",
			Message: "DateCreated is required",
		}
	}

	if o.DateCreated.After(time.Now()) {
		return ValidationError{
			Field:   "date_created",
			Message: "DateCreated cannot be in the future",
		}
	}

	// Валидация вложенных структур
	if err := o.Delivery.Validate(); err != nil {
		return err
	}
	if err := o.Payment.Validate(); err != nil {
		return err
	}

	if len(o.Items) == 0 {
		return ValidationError{
			Field:   "items",
			Message: "At least one item is required",
		}
	}

	for _, item := range o.Items {
		if err := item.Validate(); err != nil {
			return ValidationError{
				Field:   "items",
				Message: err.Error(),
			}
		}
	}

	return nil

}

// Delivery валидация доставки
func (d *Delivery) Validate() error {
	if d.Name == "" {
		return ValidationError{
			Field:   "delivery.name",
			Message: "Name cannot ge empty",
		}
	}

	if d.Phone == "" {
		return ValidationError{
			Field:   "delivery.phone",
			Message: "Phone cannot ge empty",
		}
	}

	if d.City == "" {
		return ValidationError{
			Field:   "delivery.city",
			Message: "City cannot ge empty",
		}
	}

	if d.Address == "" {
		return ValidationError{
			Field:   "delivery.address",
			Message: "Address cannot ge empty",
		}
	}

	//валидация email
	if d.Email != "" {
		if !isValidEmail(d.Email) {
			return ValidationError{
				Field:   "delivery.email",
				Message: "Invalid email format",
			}
		}
	}

	return nil
}

// Payment валидация товаров
func (p *Payment) Validate() error {
	if p.Transaction == "" {
		return ValidationError{
			Field:   "payment.transaction",
			Message: "Transaction cannot ge empty",
		}
	}

	if p.Amount <= 0 {
		return ValidationError{
			Field:   "payment.amount",
			Message: "Amount must be positive",
		}
	}

	if p.Currency == "" {
		return ValidationError{
			Field:   "payment.currency",
			Message: "Currency cannot ge empty",
		}
	}

	if p.Provider == "" {
		return ValidationError{
			Field:   "payment.Provider",
			Message: "Provider cannot ge empty",
		}
	}

	if p.PaymentDt <= 0 {
		return ValidationError{
			Field:   "payment.payment_dt",
			Message: "PaymentDt must be positive",
		}
	}

	return nil
}

// Item валидация товаров
func (i *Item) Validate() error {
	if i.ChrtID <= 0 {
		return ValidationError{
			Field:   "item.chrt_id",
			Message: "ChrtID must be positive",
		}
	}

	if i.TrackNumber == "" {
		return ValidationError{
			Field:   "item.track_number",
			Message: "TrackNumber cannot be empty",
		}
	}

	if i.Price <= 0 {
		return ValidationError{
			Field:   "item.price",
			Message: "Price must be positive",
		}
	}

	if i.TotalPrice <= 0 {
		return ValidationError{
			Field:   "item.total_price",
			Message: "TotalPrice must be positive",
		}
	}

	if i.Name == "" {
		return ValidationError{
			Field:   "item.name",
			Message: "Name cannot be empty",
		}
	}

	if i.Sale < 0 {
		return ValidationError{
			Field:   "item.sale",
			Message: "Sale cannot be negative",
		}
	}

	if i.Status < 0 {
		return ValidationError{
			Field:   "item.status",
			Message: "Status cannot be negative",
		}
	}

	return nil
}

// вспомогательная функция валидации
func isValidEmail(email string) bool {
	//простая проверка email
	for i, c := range email {
		if c == '@' && i > 0 && i < len(email)-1 {
			return true
		}
	}
	return false
}
