package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
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
	fmt.Println("Client connecting")

	amqpChannel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	gameState := gamelogic.NewGameState(userName)
	pauseQueue := fmt.Sprintf("%s.%s", routing.PauseKey, userName)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		pauseQueue,
		routing.PauseKey,
		pubsub.SimpleQueueType("transient"),
		handlerPause(gameState))
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("Client shutting down")
		os.Exit(0)
	}()

	routingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	moveArmyQueue := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		moveArmyQueue,
		routingKey,
		pubsub.SimpleQueueType("transient"),
		handlerMove(gameState, amqpChannel))
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	warRoutingKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		warRoutingKey,
		pubsub.SimpleQueueType("durable"),
		handlerWar(gameState, amqpChannel),
	)

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	for {
		fmt.Println()
		fmt.Println("Possible unit types are: infantry, cavalry, artillery")
		fmt.Println("Possible locations are: americas, europe, africa, asia, antarctica, australia")
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		word := words[0]

		if word == "spawn" {

			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Example %v\n", err)
				continue
			}

		} else if word == "move" {
			armyMove, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("Example %v\n", err)
				continue
			}

			moveRoutingKey := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName)
			err = pubsub.PublishJSON(
				amqpChannel,
				routing.ExchangePerilTopic,
				moveRoutingKey,
				armyMove,
			)
			if err != nil {
				log.Println(err)
				continue
			}
			fmt.Println("move published successfully")
		} else if word == "status" {
			gameState.CommandStatus()
		} else if word == "help" {
			gamelogic.PrintClientHelp()
		} else if word == "spam" {
			if len(words) < 2 {
				fmt.Println("Spam amount needed")
				fmt.Println("Example spam 5")
				continue
			}
			spamAmount, err := strconv.Atoi(words[1])
			if err != nil {
				log.Printf("error: %v", err)
				fmt.Println("Spam amount can be number")
				fmt.Println("Example spam 5")
				continue
			}

			for _ = range spamAmount {
				maliciousLog := gamelogic.GetMaliciousLog()
				err = pubsub.PublishGameLog(amqpChannel, userName, maliciousLog)
				if err != nil {
					log.Printf("error: %v\n", err)
				}
			}

			fmt.Printf("Published %d malicious logs\n", spamAmount)
		} else if word == "quit" {
			gamelogic.PrintQuit()
			break
		} else {
			fmt.Println("unknown command")
			continue
		}
	}

}
