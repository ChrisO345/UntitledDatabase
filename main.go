package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// MetaCommandResult Enum definition
type MetaCommandResult int

const (
	MetaCommandSuccess = MetaCommandResult(iota)
	MetaCommandUnrecognizedCommand
)

// PrepareCommandResult Enum definition
type PrepareCommandResult int

const (
	PrepareSuccess = PrepareCommandResult(iota)
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert = StatementType(iota)
	StatementSelect
)

type Statement struct {
	statementType StatementType
}

func main() {
	fmt.Println("Untitled Database Project")

	stdin := bufio.NewReader(os.Stdin)

	for {
		printPrompt()
		input := readInput(stdin)

		if strings.HasPrefix(input, ".") {
			switch handleMetaCommands(input) {
			case MetaCommandSuccess:
				continue
			case MetaCommandUnrecognizedCommand:
				fmt.Printf("Unrecognized keyword at start of '%s'\n", input)
				continue
			}
		}

		var statement Statement
		switch handlePrepareStatements(input, &statement) {
		case PrepareSuccess:
			break
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'\n", input)
			continue
		}

		executeStatement(&statement)
		fmt.Println("Executed.")
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

func handleMetaCommands(input string) MetaCommandResult {
	input = strings.TrimPrefix(input, ".")
	if input == "exit" {
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

func handlePrepareStatements(input string, statement *Statement) PrepareCommandResult {
	if strings.HasPrefix(input, "insert") {
		statement.statementType = StatementInsert
		return PrepareSuccess
	}
	if strings.HasPrefix(input, "select") {
		statement.statementType = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognizedStatement
}

func executeStatement(statement *Statement) {
	switch statement.statementType {
	case StatementInsert:
		fmt.Println("This is where we would do an insert.")
	case StatementSelect:
		fmt.Println("This is where we would do a select.")
	}
}
