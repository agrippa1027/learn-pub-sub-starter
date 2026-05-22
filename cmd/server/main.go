package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	fmt.Println("Program running. Press Ctrl+C to exit.")
	<-sigCh // blocks here until Ctrl+C or SIGTERM
	fmt.Println("\nExiting...")
}
