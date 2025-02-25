package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
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

func main() {
	// Создаем писателя для Kafka
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "orders",
		Balancer: &kafka.LeastBytes{},
	})

	products := []string{"Ноутбук", "Телефон", "Планшет", "Наушники", "Камера"}

	for i := 1; ; i++ {

		product := products[rand.Intn(len(products))]
		quanity := rand.Intn(3) + 1
		order := fmt.Sprintf(`{"Заказ": %d, "Продукт": "%s", "Количество": %d}`,
			i,
			product,
			quanity,
		)

		// Отправляем сообщение в Kafka
		err := w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("Заказ-%d", i)),
				Value: []byte(order),
			},
		)

		logEntry := LogEntry{
			Timestamp: time.Now().UTC(),
			Level:     "Инфо",
			Message:   "Заказ отправлен в Kafka",
			Service:   "producer",
			OrderID:   i,
		}

		if err != nil {
			logEntry.Level = "ошибка"
			logEntry.Message = fmt.Sprintf("ошибка отправки заказа :%s", err)
		}

		sendToElastic(logEntry)
		log.WithFields(logrus.Fields{
			"номер заказа": i,
			"продукт":      product,
			"количество":   quanity,
		}).Info("обработка заказов")

		time.Sleep(2 * time.Second)
	}
}
