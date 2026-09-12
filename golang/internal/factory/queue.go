package factory

import m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"

type MessageMiddlewareQueueRabbitMQ struct {
	queueName          string
	connectionSettings m.ConnSettings
}

func NewQueueMiddleware(queueName string, connectionSettings m.ConnSettings) MessageMiddlewareQueueRabbitMQ {
	return MessageMiddlewareQueueRabbitMQ{
		queueName,
		connectionSettings,
	}
}

func (queue MessageMiddlewareQueueRabbitMQ) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	return nil
}

func (queue MessageMiddlewareQueueRabbitMQ) StopConsuming() error {
	return nil
}

func (queue MessageMiddlewareQueueRabbitMQ) Send(msg m.Message) error {
	return nil
}

func (queue MessageMiddlewareQueueRabbitMQ) Close() error {
	return nil
}
