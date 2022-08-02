package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type Consumer interface {
	Listen(exchangeName string, topics []string, handleMessage func(message []byte)) error
}

type consumer struct {
	connection   *amqp.Connection
	queueName    string
	exchangeName string
	exchangeType string
}

func NewConsumer(connection *amqp.Connection, exchangeName string, exchangeType string) (Consumer, error) {
	c := &consumer{
		connection:   connection,
		exchangeName: exchangeName,
		exchangeType: exchangeType,
	}

	err := c.setup()

	if err != nil {
		return &consumer{}, err
	}

	return c, nil
}

func (consumer *consumer) Listen(exchangeName string, topics []string, handleMessage func(message []byte)) error {
	channel, err := consumer.connection.Channel()

	if err != nil {
		return err
	}

	defer channel.Close()

	queue, err := declareRandomQueue(channel)

	if err != nil {
		return err
	}

	for _, topic := range topics {
		err := channel.QueueBind(
			queue.Name,
			topic,
			exchangeName,
			false,
			nil,
		)

		if err != nil {
			return err
		}
	}

	messages, err := channel.Consume(queue.Name, "", true, false, false, false, nil)

	if err != nil {
		return err
	}

	forever := make(chan bool)
	go func() {
		for m := range messages {
			go handleMessage(m.Body)
		}
	}()

	log.Printf("Waiting for message on [Exchange, Queue] [%s, %s]\n", exchangeName, queue.Name)

	<-forever

	return nil
}

func (consumer *consumer) setup() error {
	channel, err := consumer.connection.Channel()

	if err != nil {
		return err
	}

	return declareExchange(channel, consumer.exchangeName, consumer.exchangeType, true, false, false)
}
