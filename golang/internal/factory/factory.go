package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrMessageMiddlewareConnect = errors.New("message middleware: connect error")
	ErrMessageMiddlewareDeclare = errors.New("message middleware: declare error")
)

func connect(connectionSettings m.ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("%s:%d", connectionSettings.Hostname, connectionSettings.Port)
	connection, err := amqp.Dial(url)

	if err != nil {
		return nil, nil, ErrMessageMiddlewareConnect
	}
	channel, err := connection.Channel()

	if err != nil {
		chErr := ErrMessageMiddlewareConnect
		closeErr := connection.Close()

		if closeErr != nil {
			closeErr = m.ErrMessageMiddlewareClose
		}
		err = errors.Join(chErr, closeErr)

		return nil, nil, err
	}
	return connection, channel, nil
}

func closeConnection(connection *amqp.Connection, channel *amqp.Channel) error {
	chErr := channel.Close()
	closeErr := connection.Close()

	if chErr != nil || closeErr != nil {
		return m.ErrMessageMiddlewareClose
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
