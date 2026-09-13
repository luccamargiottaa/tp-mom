package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageMiddlewareQueueRabbitMQ struct {
	queueName  string
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

func NewMiddlewareQueue(queueName string, connection *amqp.Connection, channel *amqp.Channel) (*MessageMiddlewareQueueRabbitMQ, error) {
	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, handleError(err, connection)
	}
	middlewareQueue := MessageMiddlewareQueueRabbitMQ{
		queueName,
		connection,
		channel,
		queue,
	}
	return &middlewareQueue, nil
}

func (middlewareQueue MessageMiddlewareQueueRabbitMQ) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	return nil
}

func (middlewareQueue MessageMiddlewareQueueRabbitMQ) StopConsuming() error {
	return nil
}

func (middlewareQueue MessageMiddlewareQueueRabbitMQ) Send(msg m.Message) error {
	return nil
}

func (middlewareQueue MessageMiddlewareQueueRabbitMQ) Close() error {
	return nil
}
