package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func declareExchange(channel *amqp.Channel, name string, exchangeType string, durable bool, autoDeleted bool, internal bool) error {
	return channel.ExchangeDeclare(
		name,
		exchangeType,
		durable,
		autoDeleted,
		internal,
		false,
		nil,
	)
}

func declareRandomQueue(channel *amqp.Channel) (amqp.Queue, error) {
	return channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
}
