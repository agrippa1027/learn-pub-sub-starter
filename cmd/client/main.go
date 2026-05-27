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
	fmt.Printf("connection established successfully to %s\n", connectionURI)

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Error receiving username\n", err)
		return
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		"peril_direct",
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		"transient",
	)
	if err != nil {
		fmt.Printf("Error declaring and binding queue: %s\n", err)
		return
	}

	gameState := gamelogic.NewGameState(username)

GAME_LLOOP:
	for {
		switch words := gamelogic.GetInput(); words[0] {

		case "spawn":
			err = gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Error processing command: %s\n", err)
				return
			}
			continue GAME_LLOOP

		case "move":
			_, err = gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("Error processing command: %s\n", err)
				return
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
