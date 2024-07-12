package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println("Untitled Database Project")

	stdin := bufio.NewReader(os.Stdin)

	for {
		printPrompt()
		input := readInput(stdin)

		if input == ".exit" {
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized command '%s'\n", input)
		}
	}
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(stdin *bufio.Reader) string {
	buffer, err := stdin.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input")
		os.Exit(1)
	}
	buffer = strings.TrimSuffix(buffer, "\r\n")
	return buffer
}
