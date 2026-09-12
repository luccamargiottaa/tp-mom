package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	queueMiddleware := NewQueueMiddleware(queueName, connectionSettings)

	return queueMiddleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	exchangeMiddleware := NewExchangeMiddleware(exchange, keys, connectionSettings)

	return exchangeMiddleware, nil
}
