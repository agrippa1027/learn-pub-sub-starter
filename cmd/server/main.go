package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/cmd/util"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	const (
		connectionURI = "amqp://guest:guest@localhost:5672/"
	)

	con, err := amqp.Dial(connectionURI)
	if err != nil {
		fmt.Printf("error establishing connection to %s\n", err)
	}
	defer con.Close()
	fmt.Printf("connection established successfully to %s\n", connectionURI)

	channel, err := con.Channel()
	if err != nil {
		fmt.Printf("error opening channel %s \n", err)
	}

	ok := pubsub.PublishJSON(
		channel,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		})

	if ok != nil {
		fmt.Printf("Message publishing failed with %s\n", ok)
	}

	pubsub.DeclareAndBind(
		con,
		"peril_topic",
		"game_logs",
		"game_logs.*",
		"durable",
	)

	gamelogic.PrintClientHelp()

GAME_LOOP:
	for {
		input := gamelogic.GetInput()

		if len(input) == 0 {
			continue
		}

		var isPause bool

		switch first := input[0]; first {
		case "pause":
			isPause = true
		case "resume":
			isPause = false
		case "quit":
			break GAME_LOOP
		default:
			fmt.Printf("Unknown command: %s\n", first)
			continue GAME_LOOP
		}

		ok := pubsub.PublishJSON(
			channel,
			routing.ExchangePerilDirect,
			routing.PauseKey,
			routing.PlayingState{
				IsPaused: isPause,
			})

		if ok != nil {
			fmt.Printf("Message publishing failed with %s\n", ok)
		}

	}

	util.InterruptHandler()
}
