package queue

import (
	"context"
	"log"
	"os"

	"github.com/gbeletti/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

var rabbit rabbitmq.RabbitMQ

func Setup(ctx context.Context) {
	rabbit = rabbitmq.NewRabbitMQ()

	confConn := rabbitmq.ConfigConnection{
		URI:           os.Getenv("RABBIT_URI"),
		PrefetchCount: 5,
	}
	var setup rabbitmq.Setup = func() {
		createQueues(rabbit)
		createConsumer(ctx, receiveMessage)
	}

	rabbitmq.KeepConnectionAndSetup(ctx, rabbit, confConn, setup)
}

func createQueues(rabbit rabbitmq.QueueCreator) {
	config := rabbitmq.ConfigQueue{
		Name:       "test",
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
	_, err := rabbit.CreateQueue(config)
	if err != nil {
		log.Printf("error creating queue: %s\n", err)
	}
}

func createConsumer(ctx context.Context, f func(d *amqp.Delivery)) {
	config := rabbitmq.ConfigConsume{
		QueueName:         "test",
		Consumer:          "test",
		AutoAck:           false,
		Exclusive:         false,
		NoLocal:           false,
		NoWait:            false,
		Args:              nil,
		ExecuteConcurrent: true,
	}

	go func() {
		if err := rabbit.Consume(ctx, config, f); err != nil {
			log.Printf("error consuming from queue: %s\n", err)
		}
	}()
}

func receiveMessage(d *amqp.Delivery) {
	defer func() {
		if err := d.Ack(false); err != nil {
			log.Printf("error acking message: %s\n", err)
		}
	}()
}
