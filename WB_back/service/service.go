package service

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"test_WB/models"
)

type OrderDB struct {
	db *sql.DB
}

func NewOrderDB(db *sql.DB) *OrderDB {
	return &OrderDB{db: db}
}

// AddDB получает данные из объекта Order добавляет в БД и в случае уже существования данных, обновляет их
func (r *OrderDB) AddDB(order *models.Order) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	err = r.insertOrder(tx, order)
	if err != nil {
		return err
	}

	err = r.insertDelivery(tx, order)
	if err != nil {
		return err
	}

	err = r.insertPayment(tx, order)
	if err != nil {
		return err
	}

	err = r.insertItems(tx, order)
	if err != nil {
		return err
	}
	return nil
}

func (r *OrderDB) insertOrder(tx *sql.Tx, order *models.Order) error {
	deleteQuery := "DELETE FROM orders WHERE order_uid = $1"
	_, err := tx.Exec(deleteQuery, order.OrderUID)
	if err != nil {
		log.Printf("Error delete old records in orders: %v\n", err)
	}
	query := `
        INSERT INTO orders (
            order_uid, track_number, entry, locale, internal_signature, 
            customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`

	_, err = tx.Exec(
		query,
		order.OrderUID,
		order.TrackNumber,
		order.Entry,
		order.Locale,
		order.InternalSignature,
		order.CustomerID,
		order.DeliveryService,
		order.ShardKey,
		order.SmID,
		order.DateCreated,
		order.OofShard,
	)
	return err
}

func (r *OrderDB) insertDelivery(tx *sql.Tx, order *models.Order) error {
	deleteQuery := "DELETE FROM delivery WHERE order_uid = $1"
	_, err := tx.Exec(deleteQuery, order.OrderUID)
	if err != nil {
		log.Printf("Error delete old records in delivery: %v\n", err)
	}
	query := `
        INSERT INTO delivery (
            order_uid, name, phone, zip, city, address, region, email
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`

	_, err = tx.Exec(
		query,
		order.OrderUID,
		order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email,
	)
	return err
}

func (r *OrderDB) insertPayment(tx *sql.Tx, order *models.Order) error {
	deleteQuery := "DELETE FROM payment WHERE order_uid = $1"
	_, err := tx.Exec(deleteQuery, order.OrderUID)
	if err != nil {
		log.Printf("Error delete old records in payment: %v\n", err)
	}
	query := `
    	INSERT INTO payment (
        	order_uid, transaction, request_id, currency, provider, 
        	amount, payment_dt, bank, delivery_cost, goods_total, custom_fee
    	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`

	_, err = tx.Exec(
		query,
		order.OrderUID,
		order.Payment.Transaction,
		order.Payment.RequestID,
		order.Payment.Currency,
		order.Payment.Provider,
		order.Payment.Amount,
		order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost,
		order.Payment.GoodsTotal,
		order.Payment.CustomFee,
	)
	return err
}

func (r *OrderDB) insertItems(tx *sql.Tx, order *models.Order) error {
	deleteQuery := "DELETE FROM items WHERE order_uid = $1"
	_, err := tx.Exec(deleteQuery, order.OrderUID)
	if err != nil {
		log.Printf("Error delete old records in items: %v\n", err)
	}
	query := `
        INSERT INTO items (
            order_uid, chrt_id, track_number, price, rid, name, 
            sale, size, total_price, nm_id, brand, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`

	for _, item := range order.Items {
		_, err := tx.Exec(
			query,
			order.OrderUID,
			item.ChrtID,
			item.TrackNumber,
			item.Price,
			item.Rid,
			item.Name,
			item.Sale,
			item.Size,
			item.TotalPrice,
			item.NmID,
			item.Brand,
			item.Status,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetOrderUID достаёт из БД данные по uid и сохраняет их в объект Order и возвращает его
func (r *OrderDB) GetOrderUID(uid string) (*models.Order, error) {
	query := `
		SELECT
			o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature,
			o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created,
			o.oof_shard, d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt,
			p.bank, p.delivery_cost, p.goods_total, p.custom_fee
		FROM orders o
		LEFT JOIN delivery d ON o.order_uid = d.order_uid
		LEFT JOIN payment p ON o.order_uid = p.order_uid
		WHERE o.order_uid = $1`

	row := r.db.QueryRow(query, uid)

	var order models.Order
	var delivery models.Delivery
	var payment models.Payment
	err := row.Scan(
		&order.OrderUID,
		&order.TrackNumber,
		&order.Entry,
		&order.Locale,
		&order.InternalSignature,
		&order.CustomerID,
		&order.DeliveryService,
		&order.ShardKey,
		&order.SmID,
		&order.DateCreated,
		&order.OofShard,
		&delivery.Name,
		&delivery.Phone,
		&delivery.Zip,
		&delivery.City,
		&delivery.Address,
		&delivery.Region,
		&delivery.Email,
		&payment.Transaction,
		&payment.RequestID,
		&payment.Currency,
		&payment.Provider,
		&payment.Amount,
		&payment.PaymentDt,
		&payment.Bank,
		&payment.DeliveryCost,
		&payment.GoodsTotal,
		&payment.CustomFee,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("UID not found: %s", uid)
			return nil, nil
		} else {
			return nil, err
		}
	}
	order.Delivery = delivery
	order.Payment = payment
	items, err := r.getItemsUID(uid)
	if err != nil {
		return nil, err
	}
	order.Items = items
	return &order, nil
}

func (r *OrderDB) getItemsUID(uid string) ([]models.Item, error) {
	query := `
		SELECT
			chrt_id, track_number, price, rid, name, sale, size, total_price,
			nm_id, brand, status
		FROM items
		WHERE order_uid = $1`

	rows, err := r.db.Query(query, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []models.Item
	for rows.Next() {
		var item models.Item
		err := rows.Scan(
			&item.ChrtID,
			&item.TrackNumber,
			&item.Price,
			&item.Rid,
			&item.Name,
			&item.Sale,
			&item.Size,
			&item.TotalPrice,
			&item.NmID,
			&item.Brand,
			&item.Status,
		)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (r *OrderDB) GetUIDPerTime(time int) ([]string, error) {
	query := `SELECT order_uid FROM orders;`
	rows, err := r.db.Query(query, time)
	if err != nil {
		return nil, fmt.Errorf("error querying order UIDs: %v", err)
	}
	defer rows.Close()

	var orderUIDs []string
	for rows.Next() {
		var orderUID string
		err := rows.Scan(&orderUID)
		if err != nil {
			return nil, fmt.Errorf("error scanning order UID: %w", err)
		}
		orderUIDs = append(orderUIDs, orderUID)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	if len(orderUIDs) == 0 {
		log.Printf("No entries found for the last %d hours", time)
		return nil, nil
	}

	return orderUIDs, nil
}
