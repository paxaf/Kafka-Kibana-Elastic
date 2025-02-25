package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var (
	esClient *elasticsearch.Client
	log      = logrus.New()
)

type LogEntry struct {
	Timestamp time.Time `json:"@timestamp"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Service   string    `json:"service"`
	OrderID   int       `json:"order_id,omitempty"`
}

func sendToElastic(entry LogEntry) {
	body, _ := json.Marshal(entry)

	_, err := esClient.Index(
		"kafka-logs",
		strings.NewReader(string(body)),
		esClient.Index.WithRefresh("true"),
	)

	if err != nil {
		log.Errorf("Ошибка отправка лога в ElasticSearch: %s", err)
	}
}

func init() {
	// Инициализация Elasticsearch
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	}

	var err error
	esClient, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Ошибка создания клиента Elasticsearch: %s", err)
	}

	// Настройка логгера
	log.SetFormatter(&logrus.JSONFormatter{})
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "orders",
		GroupID:  "order-processors",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			logEntry := LogEntry{
				Timestamp: time.Now().UTC(),
				Level:     "ошибка",
				Message:   fmt.Sprintf("ошибка чтения сообщения: %s", err),
				Service:   "consumer",
			}
			sendToElastic(logEntry)
			continue
		}
		logEntry := LogEntry{
			Timestamp: time.Now().UTC(),
			Level:     "Инфо",
			Message:   "Заказ доставлен",
			Service:   "consumer",
		}
		sendToElastic(logEntry)
		fmt.Printf("Заказ доставлен: %s\n", string(msg.Value))
	}
}
