package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGOB[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer

	encoder := gob.NewEncoder(&buf)

	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	var msg amqp.Publishing
	msg.ContentType = "application/gob"
	msg.Body = buf.Bytes()

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
	if err != nil {
		return err
	}
	return nil
}

func SubscribeGOB[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	amqpChannel, amqpQueue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	amqpDeliveryChan, err := amqpChannel.Consume(amqpQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range amqpDeliveryChan {
			var value T
			decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
			err := decoder.Decode(&value)
			if err != nil {
				log.Println(err)
			}
			resultAck := handler(value)
			if resultAck == Ack {
				err = msg.Ack(false)
				if err != nil {
					log.Println("message ack")
					log.Println(err)
				}
			} else if resultAck == NackRequeue {
				err = msg.Nack(false, true)
				if err != nil {
					log.Println("message nack requeue")
					log.Println(err)
				}
			} else {
				err = msg.Nack(false, false)
				log.Println("message nack no-requeue")
				if err != nil {
					log.Println(err)
				}
			}

		}
	}()

	return nil

}

func PublishGameLog(ch *amqp.Channel, username, message string) error {
	routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)

	return PublishGOB(
		ch,
		routing.ExchangePerilTopic,
		routingKey,
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     message,
			Username:    username,
		},
	)
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	valBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	var msg amqp.Publishing
	msg.ContentType = "application/json"
	msg.Body = valBytes

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)

	if err != nil {
		return err
	}
	return nil
}

type AckType string

const (
	Ack         AckType = "Ack"
	NackRequeue AckType = "NackRequeue"
	NackDiscard AckType = "NackDiscard"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	amqpChannel, amqpQueue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	amqpDeliveryChan, err := amqpChannel.Consume(amqpQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range amqpDeliveryChan {
			var value T
			err := json.Unmarshal(msg.Body, &value)
			if err != nil {
				log.Println(err)
			}
			resultAck := handler(value)
			if resultAck == Ack {
				err = msg.Ack(false)
				if err != nil {
					log.Println("message ack")
					log.Println(err)
				}
			} else if resultAck == NackRequeue {
				err = msg.Nack(false, true)
				if err != nil {
					log.Println("message nack requeue")
					log.Println(err)
				}
			} else {
				err = msg.Nack(false, false)
				log.Println("message nack no-requeue")
				if err != nil {
					log.Println(err)
				}
			}

		}
	}()

	return nil
}

type SimpleQueueType string

const (
	durable   SimpleQueueType = "durable"
	transient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	amqpChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = amqpChannel.Qos(10, 0, false)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	isDurable := false
	autoDeleTe := true
	exclusive := true
	noWait := false
	if queueType == durable {
		isDurable = true
		autoDeleTe = false
		exclusive = false
	}
	amqpQueue, err := amqpChannel.QueueDeclare(queueName, isDurable, autoDeleTe, exclusive, noWait, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})

	if err != nil {
		return nil, amqp.Queue{}, err
	}
	err = amqpChannel.QueueBind(queueName, key, exchange, noWait, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return amqpChannel, amqpQueue, nil
}
