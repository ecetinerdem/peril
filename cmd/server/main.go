package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(conStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("Starting Peril server...")

	amqpChannel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("Shutting down Peril server...")
		os.Exit(0)
	}()

	err = pubsub.SubscribeGOB(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,      // queue name
		routing.GameLogSlug+".*", // wildcard routing key to catch all usernames
		pubsub.SimpleQueueType("durable"),
		handlerLog(),
	)
	if err != nil {
		log.Fatal(err)
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		word := words[0]
		if word == "pause" {
			log.Println("sending pause message")

			err = pubsub.PublishJSON(
				amqpChannel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				log.Println(err)
			}
		} else if word == "resume" {
			log.Println("sending resume message")

			err = pubsub.PublishJSON(
				amqpChannel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				log.Println(err)
			}
		} else if word == "quit" {
			log.Println("exiting the game loop")
			break
		} else {
			log.Println("unknown command")
		}
	}

}
