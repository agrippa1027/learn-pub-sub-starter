package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	}

	pubsub.DeclareAndBind(
		conn,
		"peril_direct",
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		"transient",
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	fmt.Println("Program running. Press Ctrl+C to exit.")
	<-sigCh // blocks here until Ctrl+C or SIGTERM
	fmt.Println("\nExiting...")
}
