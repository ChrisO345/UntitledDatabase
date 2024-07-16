package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	columnUsernameSize = 32
	columnEmailSize    = 255
	idSize             = 4
	usernameSize       = 32
	emailSize          = 369
	idOffset           = 0
	usernameOffset     = idOffset + idSize
	emailOffset        = usernameOffset + usernameSize
	rowSize            = emailOffset + emailSize
	pageSize           = 4096
	tableMaxPages      = 100
	rowsPerPage        = pageSize / rowSize
	tableMaxRows       = rowsPerPage * tableMaxPages
)

// MetaCommandResult Enum definition
type MetaCommandResult int

const (
	MetaCommandSuccess = MetaCommandResult(iota)
	MetaCommandUnrecognizedCommand
)

func handleMetaCommands(input string, table *Table) MetaCommandResult {
	input = strings.TrimPrefix(input, ".")
	if input == "exit" {
		dbClose(table)
		os.Exit(0)
	}
	return MetaCommandUnrecognizedCommand
}

// PrepareCommandResult Enum definition
type PrepareCommandResult int

const (
	PrepareSuccess = PrepareCommandResult(iota)
	PrepareStringTooLong
	PrepareNegativeId
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
	Username [columnUsernameSize]byte
	Email    [columnEmailSize]byte
}

type Statement struct {
	statementType StatementType
	rowToInsert   Row // Only used by insert statement
}

type Pager struct {
	file       *os.File
	fileLength int64
	pages      [tableMaxPages][]byte
}

type Table struct {
	numRows uint32
	pager   *Pager
}

type Cursor struct {
	table      *Table
	rowNum     uint32
	endOfTable bool // Indicates the position one after the last row in the table
}

func pagerOpen(filename string) *Pager {
	content, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Error opening file")
		os.Exit(1)
	}

	fi, err := content.Stat()
	if err != nil {
		fmt.Println("Error getting file info")
		os.Exit(1)
	}

	fileLength := fi.Size()

	pager := &Pager{}
	pager.fileLength = fileLength
	pager.file = content

	for i := 0; i < tableMaxPages; i++ {
		pager.pages[i] = nil
	}

	return pager
}

func dbOpen(filename string) *Table {
	// Opens database file
	// Initializes the pager data structure
	// Initializes the table data structure
	pager := pagerOpen(filename)
	numRows := pager.fileLength / rowSize

	table := Table{numRows: 0}
	table.pager = pager
	table.numRows = uint32(numRows)

	return &table
}

func pagerFlush(pager *Pager, pageIndex uint32, size uint32) {
	if pager.pages[pageIndex] == nil {
		fmt.Println("Tried to flush null page")
		os.Exit(1)
	}

	//offset := pageIndex * pageSize
	//_, err := pager.file.WriteAt(pager.pages[pageIndex], int64(offset))
	//_, err := pager.file.Write(pager.pages[pageIndex][:size])
	_, err := pager.file.Write(pager.pages[pageIndex][:size])
	if err != nil {
		fmt.Println("Error writing to disk")
		os.Exit(1)
	}
}

func dbClose(table *Table) {
	// Close the file
	// Free the pager
	// Free the table
	pager := table.pager
	numFullPages := table.numRows / rowsPerPage

	err := pager.file.Truncate(0)

	if err != nil {
		fmt.Println("Error truncating file")
		os.Exit(1)
	}

	_, err = pager.file.Seek(0, 0)
	if err != nil {
		fmt.Println("Error seeking file")
		os.Exit(1)
	}

	var i uint32
	for i = 0; i < numFullPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		// Flush page to disk
		// Free page from memory
		pagerFlush(pager, i, pageSize)
		pager.pages[i] = nil
	}

	numAdditionalRows := table.numRows % rowsPerPage
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.pages[pageNum] != nil {
			pagerFlush(pager, pageNum, numAdditionalRows*rowSize)
			pager.pages[pageNum] = nil
		}
	}

	err = pager.file.Close()

	if err != nil {
		fmt.Println("Error closing db file")
		os.Exit(1)
	}

	for i = 0; i < tableMaxPages; i++ {
		pager.pages[i] = nil
	}
}

func prepareInsert(input string, statement *Statement) PrepareCommandResult {
	statement.statementType = StatementInsert

	argsAssigned := strings.Split(input, " ")

	if len(argsAssigned) != 4 {
		fmt.Println("Error parsing input")
		return PrepareSyntaxError
	}

	id, err := strconv.Atoi(argsAssigned[1])
	if err != nil {
		fmt.Println("Error parsing ID")
		return PrepareSyntaxError
	}

	if id < 0 {
		return PrepareNegativeId
	}

	if len(argsAssigned[2]) > columnUsernameSize {
		return PrepareStringTooLong
	}

	if len(argsAssigned[2]) > columnEmailSize {
		return PrepareStringTooLong
	}

	statement.rowToInsert.Id = int32(id)
	copy(statement.rowToInsert.Username[:], argsAssigned[2])
	copy(statement.rowToInsert.Email[:], argsAssigned[3])

	return PrepareSuccess
}

func handlePrepareStatements(input string, statement *Statement) PrepareCommandResult {
	if strings.HasPrefix(input, "insert") {
		return prepareInsert(input, statement)
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

func getPage(pager *Pager, pageIndex uint32) []byte {
	if pageIndex > tableMaxPages {
		fmt.Printf("Tried to fetch page number out of bounds. %d > %d\n", pageIndex, tableMaxPages)
		os.Exit(1)
	}

	if pager.pages[pageIndex] == nil {
		// Allocate memory for page
		pager.pages[pageIndex] = make([]byte, pageSize)

		if pager.fileLength > 0 {
			// Read in page
			_, err := pager.file.Read(pager.pages[pageIndex])

			if err != nil {
				fmt.Println("Error reading file")
				fmt.Println(err)
				os.Exit(1)
			}
		}
	}

	return pager.pages[pageIndex]
}

func cursorValue(cursor *Cursor) []byte {
	pageIndex := cursor.rowNum / rowsPerPage
	page := getPage(cursor.table.pager, pageIndex)

	rowOffset := cursor.rowNum % rowsPerPage
	byteOffset := rowOffset * rowSize
	result := page[byteOffset : byteOffset+rowSize]
	return result
}

func rowSlot(table *Table, rowNum uint32) []byte {
	pageIndex := rowNum / rowsPerPage

	page := getPage(table.pager, pageIndex)

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
	cursor := tableEnd(table)

	serializeRow(rowToInsert, cursorValue(cursor))
	// Serialize row (source, destination)
	// rowToInsert, rowSlot(table, table.numRows) -> Buffer?? pointer

	table.numRows++
	return ExecuteSuccess
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.Id, row.Username, row.Email)
}

func executeSelect(table *Table) ExecuteCommandResult {
	cursor := tableStart(table)

	var row Row

	for !cursor.endOfTable {
		deserializeRow(cursorValue(cursor), &row)
		printRow(&row)
		advanceCursorPosition(cursor)
	}
	return ExecuteSuccess
}

func executeStatement(statement *Statement, table *Table) ExecuteCommandResult {
	switch statement.statementType {
	case StatementInsert:
		return executeInsert(statement, table)
	case StatementSelect:
		return executeSelect(table)
	}
	return ExecuteTableFull
}

func tableStart(table *Table) *Cursor {
	cursor := Cursor{table: table, rowNum: 0, endOfTable: table.numRows == 0}

	return &cursor
}

func tableEnd(table *Table) *Cursor {
	cursor := Cursor{table: table, rowNum: table.numRows, endOfTable: true}

	return &cursor
}

func advanceCursorPosition(cursor *Cursor) {
	cursor.rowNum++

	if cursor.rowNum >= cursor.table.numRows {
		cursor.endOfTable = true
	}
}

func main() {
	fmt.Println("Untitled Database Project")
	if len(os.Args) < 2 {
		fmt.Println("Must supply a database filename.")
		os.Exit(1)
	}
	filename := os.Args[1]
	table := dbOpen(filename)
	stdin := bufio.NewReader(os.Stdin)

	for {
		printPrompt()
		input := readInput(stdin)

		if strings.HasPrefix(input, ".") {
			switch handleMetaCommands(input, table) {
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
		case PrepareStringTooLong:
			fmt.Println("String is too long")
			continue
		case PrepareNegativeId:
			fmt.Println("ID must be positive")
			continue
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
