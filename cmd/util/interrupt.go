package util

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func InterruptHandler() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	fmt.Println("Program running. Press Ctrl+C to exit.")
	<-sigCh // blocks here until Ctrl+C or SIGTERM
	fmt.Println("\nExiting...")
}
