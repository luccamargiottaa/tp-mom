package factory

import (
	"sync/atomic"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const prefetchCount = 1

type MessageMiddlewareQueueRabbitMQ struct {
	queueName  string
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
	consuming  atomic.Bool
}

func ack(delivery amqp.Delivery) {
	_ = delivery.Ack(false)
}

func nack(delivery amqp.Delivery) {
	_ = delivery.Nack(false, true)
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
	if err = channel.Qos(prefetchCount, 0, false); err != nil {
		return nil, handleError(err, connection)
	}
	middlewareQueue := MessageMiddlewareQueueRabbitMQ{
		queueName:  queueName,
		connection: connection,
		channel:    channel,
		queue:      queue,
	}
	return &middlewareQueue, nil
}

func (middlewareQueue *MessageMiddlewareQueueRabbitMQ) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	deliveries, err := middlewareQueue.channel.Consume(
		middlewareQueue.queueName,
		middlewareQueue.queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return handleError(err, middlewareQueue.connection)
	}
	middlewareQueue.consuming.Store(true)

	for delivery := range deliveries {
		message := m.Message{
			Body: string(delivery.Body),
		}
		callbackFunc(
			message,
			func() { ack(delivery) },
			func() { nack(delivery) },
		)
	}
	if middlewareQueue.consuming.Load() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (middlewareQueue *MessageMiddlewareQueueRabbitMQ) StopConsuming() error {
	if !middlewareQueue.consuming.Load() {
		return nil
	}
	middlewareQueue.consuming.Store(false)
	err := middlewareQueue.channel.Cancel(middlewareQueue.queueName, false)

	if err != nil {
		return handleError(err, middlewareQueue.connection)
	}
	return nil
}

func (middlewareQueue *MessageMiddlewareQueueRabbitMQ) Send(msg m.Message) error {
	sentMessage := amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(msg.Body),
	}
	err := middlewareQueue.channel.Publish(
		"",
		middlewareQueue.queueName,
		false,
		false,
		sentMessage,
	)
	if err != nil {
		return handleError(err, middlewareQueue.connection)
	}
	return nil
}

func (middlewareQueue *MessageMiddlewareQueueRabbitMQ) Close() error {
	channelErr := middlewareQueue.channel.Close()
	connectionErr := closeConnection(middlewareQueue.connection)

	if channelErr != nil || connectionErr != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
