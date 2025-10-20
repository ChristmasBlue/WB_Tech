package config

import (
	"os"
)

type Config struct {
	ServerAddress  string
	BrokersAddress string
	GroupIdGet     string
	GroupIdAdd     string
	Topic          string
	KeyGetOrder    string
	KeyAddOrder    string
	KeySendOrder   string
	DbType         string
	DbHost         string
	DbUser         string
	DbPassword     string
	DbName         string
	DbSslmode      string
	DbPort         string
}

func Load() *Config {
	cfg := &Config{
		ServerAddress:  os.Getenv("SERVER_ADDRESS"),
		BrokersAddress: os.Getenv("KAFKA_BROKERS"),
		Topic:          os.Getenv("KAFKA_TOPIC"),
		GroupIdGet:     os.Getenv("KAFKA_GROUP_ID_GET"),
		GroupIdAdd:     os.Getenv("KAFKA_GROUP_ID_ADD"),
		KeyGetOrder:    os.Getenv("KAFKA_KEY_GET_ORDER"),
		KeyAddOrder:    os.Getenv("KAFKA_KEY_ADD_ORDER"),
		KeySendOrder:   os.Getenv("KAFKA_KEY_SEND_ORDER"),
		DbType:         os.Getenv("DB_TYPE"),
		DbHost:         os.Getenv("DB_HOST"),
		DbPort:         os.Getenv("DB_PORT"),
		DbUser:         os.Getenv("DB_USER"),
		DbPassword:     os.Getenv("DB_PASSWORD"),
		DbName:         os.Getenv("DB_NAME"),
		DbSslmode:      os.Getenv("DB_SSL_MODE"),
	}
	return cfg
}
