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
	fmt.Println("Starting Peril client...")

	const (
		connectionURI = "amqp://guest:guest@localhost:5672/"
	)

	conn, err := amqp.Dial(connectionURI)
	if err != nil {
		fmt.Printf("error establishing connection to %s\n", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("error opening channel %s \n", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Error receiving username\n", err)
		return
	}

	gameState := gamelogic.NewGameState(username)

	go func() {
		err = pubsub.SubscribeJSON(
			conn,
			routing.ExchangePerilDirect,
			fmt.Sprintf("%s.%s", routing.PauseKey, username),
			routing.PauseKey,
			"transient",
			handlerPause(gameState),
		)
		if err != nil {
			fmt.Printf("Error subscribing, declaring and binding queue: %s\n", err)
			return
		}
	}()

	go func() {
		err = pubsub.SubscribeJSON(
			conn,
			routing.ExchangePerilTopic,
			fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
			fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
			"transient",
			handlerArmyMove(gameState),
		)
		if err != nil {
			fmt.Printf("Error subscribing, declaring and binding queue: %s\n", err)
			return
		}
	}()

GAME_LLOOP:
	for {
		switch words := gamelogic.GetInput(); words[0] {
		case "spawn":
			err = gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Error processing command: %s\n", err)
			}
			continue GAME_LLOOP

		case "move":
			am, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("Error processing command: %s\n", err)
			}

			ok := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				am,
			)
			if ok != nil {
				fmt.Printf("Message publishing failed with %s\n", ok)
			}
			continue GAME_LLOOP

		case "status":
			gameState.CommandStatus()
			continue GAME_LLOOP

		case "help":
			gamelogic.PrintClientHelp()
			continue GAME_LLOOP

		case "spam":
			fmt.Println("Spamming not allowed yet!")
			continue GAME_LLOOP

		case "quit":
			gamelogic.PrintQuit()
			break GAME_LLOOP

		default:
			fmt.Printf("Unknown command: %s\n", words[0])
			break GAME_LLOOP
		}
	}

	util.InterruptHandler()
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	defer fmt.Print("> ")

	return func(ps routing.PlayingState) {
		gs.HandlePause(ps)
	}
}

func handlerArmyMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	defer fmt.Print("> ")

	return func(am gamelogic.ArmyMove) {
		gs.HandleMove(am)
	}
}
