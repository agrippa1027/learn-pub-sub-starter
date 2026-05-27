package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

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
		queueType.durable(), queueType.autoDelete(), queueType.exlusive(),
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	ok := channel.QueueBind(
		queueName,
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	var err error
	channel, _, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return fmt.Errorf("error declaring and binding queue: %s", err)
	}

	dChannel, err := channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error consuming from queue: %s", err)
	}

	for delivery := range dChannel {
		var body T
		err = json.Unmarshal(delivery.Body, &body)
		if err != nil {
			return fmt.Errorf("error unmarshal body: %s", err)
		}
		handler(body)
		delivery.Ack(false)
	}

	return nil
}
