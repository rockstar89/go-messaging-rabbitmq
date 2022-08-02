package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math"
	"time"
)

type Client interface {
	Connect() (*amqp.Connection, error)
}

type client struct {
	username string
	password string
}

func NewRabbitMqClient(username string, password string) Client {
	return &client{
		username,
		password,
	}
}

func (client *client) Connect() (*amqp.Connection, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	for {
		c, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@rabbitmq", client.username, client.password))

		if err != nil {
			log.Println("RabbitMq not ready yet...")
			counts++
		} else {
			connection = c
			break
		}

		if counts > 5 {
			log.Println(err)
			return nil, err
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Println("Backing off")

		time.Sleep(backOff)
		continue
	}

	return connection, nil
}
