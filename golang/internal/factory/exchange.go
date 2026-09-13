package factory

import (
	"errors"

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
		if err = closeConnection(connection); err != nil {
			return nil, err
		}
		if errors.Is(err, amqp.ErrClosed) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		return nil, m.ErrMessageMiddlewareMessage
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
