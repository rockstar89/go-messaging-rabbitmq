package rabbitmq

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type Emitter interface {
	Emit(message Message, route string) error
}

type emitter struct {
	connection   *amqp.Connection
	exchangeName string
	exchangeType string
}

func NewEmitter(connection *amqp.Connection, exchangeName string, exchangeType string) (Emitter, error) {
	e := &emitter{
		connection:   connection,
		exchangeName: exchangeName,
		exchangeType: exchangeType,
	}

	err := e.setup()

	if err != nil {
		return &emitter{}, nil
	}

	return e, nil
}

func (emitter *emitter) Emit(message Message, route string) error {
	channel, err := emitter.connection.Channel()

	if err != nil {
		return err
	}

	defer channel.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()

	log.Println("Publishing to channel!")

	event, err := json.MarshalIndent(&message, "", "\t")

	if err != nil {
		return err
	}

	err = channel.PublishWithContext(ctx, emitter.exchangeName, route, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        event,
	})

	if err != nil {
		return err
	}

	return nil
}

func (emitter *emitter) setup() error {
	channel, err := emitter.connection.Channel()

	if err != nil {
		return err
	}

	defer channel.Close()

	return declareExchange(channel, emitter.exchangeName, emitter.exchangeType, true, false, false)
}
