package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
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

func handleMetaCommands(input string) MetaCommandResult {
	input = strings.TrimPrefix(input, ".")
	if input == "exit" {
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

// PrepareCommandResult Enum definition
type PrepareCommandResult int

const (
	PrepareSuccess = PrepareCommandResult(iota)
	PrepareSyntaxError
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert = StatementType(iota)
	StatementSelect
)

type Row struct {
	Id       int32
	Username [32]byte
	Email    [255]byte
}

type Statement struct {
	statementType StatementType
	rowToInsert   Row // Only used by insert statement
}

const (
	idOffset       = 0
	usernameOffset = idOffset + 4
	emailOffset    = usernameOffset + 32
	rowSize        = emailOffset + 369
	pageSize       = 4096
	rowsPerPage    = pageSize / rowSize
	tableMaxRows   = rowsPerPage * 100
)

type Table struct {
	numRows uint32
	pages   [100][]byte
}

// I think that there is an issue with this
func newTable() *Table {
	table := Table{numRows: 0}
	for i := 0; i < 100; i++ {
		table.pages[i] = nil
	}
	return &table
}

func handlePrepareStatements(input string, statement *Statement) PrepareCommandResult {
	if strings.HasPrefix(input, "insert") {
		statement.statementType = StatementInsert
		var (
			username string
			email    string
		)
		argsAssigned, err := fmt.Sscanf(input, "insert %d %s %s", &statement.rowToInsert.Id,
			&username, &email)
		copy(statement.rowToInsert.Username[:], username)
		copy(statement.rowToInsert.Email[:], email)
		if err != nil {
			fmt.Println("Error parsing input")
			return PrepareSyntaxError
		}
		if argsAssigned < 3 {
			return PrepareSyntaxError
		}
		return PrepareSuccess
	}
	if strings.HasPrefix(input, "select") {
		statement.statementType = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognizedStatement
}

type ExecuteCommandResult int

const (
	ExecuteSuccess = ExecuteCommandResult(iota)
	ExecuteTableFull
)

func serializeRow(source *Row, destination []byte) {
	// Serialize row
	// source, destination
	gob.Register(source)
	gob.Register(destination)
	buffer := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buffer)

	err := encoder.Encode(&source)
	if err != nil {
		fmt.Println("Error serialising row")
		fmt.Println(err)
		os.Exit(1)
	}

	copy(destination, buffer.Bytes())
}

func deserializeRow(source []byte, destination *Row) {
	// Deserialize row
	// source, destination
	buffer := bytes.NewBuffer(source)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&destination)
	if err != nil {
		fmt.Println("Error de-serialising row")
		fmt.Println(err)
		//os.Exit(1)
	}
}

func rowSlot(table *Table, rowNum uint32) []byte {
	pageIndex := rowNum / rowsPerPage

	// Get Current Page from index
	page := table.pages[pageIndex]

	if page == nil {
		// Allocate memory for page
		page = make([]byte, pageSize)
		table.pages[pageIndex] = page
	}

	rowOffset := rowNum % rowsPerPage
	byteOffset := rowOffset * rowSize
	result := page[byteOffset : byteOffset+rowSize]
	return result
}

func executeInsert(statement *Statement, table *Table) ExecuteCommandResult {
	if table.numRows >= tableMaxRows {
		return ExecuteTableFull
	}

	rowToInsert := &statement.rowToInsert
	serializeRow(rowToInsert, rowSlot(table, table.numRows))
	// Serialize row (source, destination)
	// rowToInsert, rowSlot(table, table.numRows) -> Buffer?? pointer

	table.numRows++
	return ExecuteSuccess
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.Id, row.Username, row.Email)
}

func executeSelect(statement *Statement, table *Table) ExecuteCommandResult {
	var row Row
	var i uint32
	for i = 0; i < table.numRows; i++ {
		deserializeRow(rowSlot(table, i), &row)
		printRow(&row)
	}
	return ExecuteSuccess
}

func executeStatement(statement *Statement, table *Table) ExecuteCommandResult {
	switch statement.statementType {
	case StatementInsert:
		return executeInsert(statement, table)
	case StatementSelect:
		return executeSelect(statement, table)
	}
	return ExecuteTableFull
}

func main() {
	fmt.Println("Untitled Database Project")

	table := newTable()
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
		//goland:noinspection GoSwitchMissingCasesForIotaConsts
		switch handlePrepareStatements(input, &statement) {
		case PrepareSuccess:
			break
		case PrepareSyntaxError:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'\n", input)
			continue
		}

		//executeStatement(&statement)
		switch executeStatement(&statement, table) {
		case ExecuteSuccess:
			fmt.Println("Executed.")
		case ExecuteTableFull:
			fmt.Println("Error: Table full.")
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
