package factory

import m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"

type MessageMiddlewareExchangeRabbitMQ struct {
	exchange           string
	keys               []string
	connectionSettings m.ConnSettings
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) MessageMiddlewareExchangeRabbitMQ {
	return MessageMiddlewareExchangeRabbitMQ{
		exchange,
		keys,
		connectionSettings,
	}
}

func (exchange MessageMiddlewareExchangeRabbitMQ) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	return nil
}

func (exchange MessageMiddlewareExchangeRabbitMQ) StopConsuming() error {
	return nil
}

func (exchange MessageMiddlewareExchangeRabbitMQ) Send(msg m.Message) error {
	return nil
}

func (exchange MessageMiddlewareExchangeRabbitMQ) Close() error {
	return nil
}
