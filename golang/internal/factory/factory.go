package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	connection, channel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return newMiddlewareQueue(queueName, connection, channel)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	connection, channel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return newMiddlewareExchange(exchange, keys, connection, channel)
}
