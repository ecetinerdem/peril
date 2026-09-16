package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {

	return func(r routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(r)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, amqpChannel *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			userName := gs.GetUsername()
			moveRoutingKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, userName)

			err := pubsub.PublishJSON(
				amqpChannel,
				routing.ExchangePerilTopic,
				moveRoutingKey,
				gamelogic.RecognitionOfWar{
					Attacker: am.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				log.Println(err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, amqpChannel *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {

	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		var logMessage string
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			logMessage = fmt.Sprintf("%s won a war against %s", winner, loser)
		case gamelogic.WarOutcomeYouWon:
			logMessage = fmt.Sprintf("%s won a war against %s", winner, loser)
		case gamelogic.WarOutcomeDraw:
			logMessage = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
		default:
			log.Println("error: unknown war outcome")
			return pubsub.NackDiscard
		}

		logRoutingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, gs.GetUsername())
		err := pubsub.PublishGOB(
			amqpChannel,
			routing.ExchangePerilTopic,
			logRoutingKey,
			routing.GameLog{
				CurrentTime: time.Now(),
				Message:     logMessage,
				Username:    gs.GetUsername(),
			},
		)
		if err != nil {
			log.Println(err)
		}

		return pubsub.Ack

	}
}
