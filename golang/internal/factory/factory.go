package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func connect(connectionSettings m.ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("%s:%d", connectionSettings.Hostname, connectionSettings.Port)
	connection, err := amqp.Dial(url)

	if err != nil {
		return nil, nil, m.ErrMessageMiddlewareDisconnected
	}
	channel, err := connection.Channel()

	if err != nil {
		return nil, nil, handleError(err, connection)
	}
	return connection, channel, nil
}

func handleError(err error, connection *amqp.Connection) error {
	if err = closeConnection(connection); err != nil {
		return err
	}
	if errors.Is(err, amqp.ErrClosed) {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

func closeConnection(connection *amqp.Connection) error {
	if !connection.IsClosed() {
		if err := connection.Close(); err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}
	return nil
}

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	connection, channel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return NewMiddlewareQueue(queueName, connection, channel)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	connection, channel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return NewMiddlewareExchange(exchange, keys, connection, channel)
}
