package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageMiddlewareExchangeRabbitMQ struct {
	exchange   string
	keys       []string
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewMiddlewareExchange(exchange string, keys []string, connection *amqp.Connection, channel *amqp.Channel) (*MessageMiddlewareExchangeRabbitMQ, error) {
	err := channel.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, handleError(err, connection)
	}
	middlewareExchange := MessageMiddlewareExchangeRabbitMQ{
		exchange,
		keys,
		connection,
		channel,
	}
	return &middlewareExchange, nil
}

func (middlewareExchange MessageMiddlewareExchangeRabbitMQ) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	return nil
}

func (middlewareExchange MessageMiddlewareExchangeRabbitMQ) StopConsuming() error {
	return nil
}

func (middlewareExchange MessageMiddlewareExchangeRabbitMQ) Send(msg m.Message) error {
	return nil
}

func (middlewareExchange MessageMiddlewareExchangeRabbitMQ) Close() error {
	return nil
}
