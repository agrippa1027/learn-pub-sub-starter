package pubsub

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](
	ch *amqp.Channel,
	exchange,
	key string,
	val T,
) error {
	message, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})

	return err
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := channel.QueueDeclare(
		queueName,
		queueType.durable(),
		queueType.autoDelete(),
		queueType.exlusive(),
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	ok := channel.QueueBind(
		uuid.New().String(),
		key,
		exchange,
		false,
		nil,
	)

	if ok != nil {
		return nil, amqp.Queue{}, ok
	}

	return channel, queue, nil
}
