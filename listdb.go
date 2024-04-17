package main

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/tidwall/redcon"
)

type ListDB struct {
	mu   sync.RWMutex
	data map[string][]string
}

func NewListDB() *ListDB {
	return &ListDB{
		data: make(map[string][]string),
	}
}

func set_at_index(list *[]string, index int, value string) int {
	if index > len(*list) {
		*list = []string{value}
		return 1
	}

	*list = append((*list)[:index+1], (*list)[index:]...)
	(*list)[index] = value

	return index
}

func (ldb *ListDB) lpush(conn redcon.Conn, cmd redcon.Command) {
	fmt.Println("pushing...")
	if len(cmd.Args) != 3 {
		conn.WriteError("Invalid arguments number for " + string(cmd.Args[0]) + " command")
		return
	}

	func() {
		ldb.mu.Lock()
		defer ldb.mu.Unlock()
		data, exist := ldb.data[string(cmd.Args[1])]
		if !exist {
			data = []string{string(cmd.Args[2])}
		} else {
			set_at_index(&data, 0, string(cmd.Args[2]))
		}
		ldb.data[string(cmd.Args[1])] = data
	}()

	conn.WriteString("1")
}

func (ldb *ListDB) rpush(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("Invalid arguments number for " + string(cmd.Args[0]) + " command")
		return
	}

	func() {
		ldb.mu.Lock()
		defer ldb.mu.Unlock()
		data, _ := ldb.data[string(cmd.Args[1])]
		data = append(data, string(cmd.Args[2]))
		ldb.data[string(cmd.Args[1])] = data
	}()

	conn.WriteString("1")
}

func (ldb *ListDB) get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("Invalid arguments number for " + string(cmd.Args[0]) + " command")
		return
	}

	index, _ := strconv.Atoi(string(cmd.Args[2]))
	ldb.mu.RLock()
	defer ldb.mu.RUnlock()
	if index > len(ldb.data[string(cmd.Args[1])]) {
		conn.WriteNull()
		return
	}
	data, ok := ldb.data[string(cmd.Args[1])]
	fmt.Println(data[index])

	if !ok {
		conn.WriteNull()
	} else {
		conn.WriteString(data[index])
	}
}

func (ldb *ListDB) pop(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("Invalid arguments number for " + string(cmd.Args[0]) + " command")
		return
	}

	ldb.mu.Lock()
	defer ldb.mu.Unlock()
	data, _ := ldb.data[string(cmd.Args[1])]
	ldb.data[string(cmd.Args[1])] = data[1:]

	conn.WriteString(data[0])
}
