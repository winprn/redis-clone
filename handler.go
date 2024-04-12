package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/redcon"
)

type Data struct {
	content string
	ttl     int
}

type DB struct {
	mu   sync.RWMutex
	data map[string]Data
	wg   *sync.WaitGroup
}

func NewDB() *DB {
	return &DB{
		data: make(map[string]Data),
	}
}

func (h *DB) ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (h *DB) set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("Invalid number of arguments for " + string(cmd.Args[0]) + " command")
	}

	h.mu.Lock()
	data, _ := h.data[string(cmd.Args[1])]
	data.content = string(cmd.Args[2])
	data.ttl = -1
	h.data[string(cmd.Args[1])] = data
	h.mu.Unlock()

	conn.WriteString("OK")
}

func (h *DB) get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	// prevent race condition between concurrent read and write requests
	// RLock allows multiple go routines to read at the same time
	// so there won't be any bottleneck
	h.mu.RLock()
	val, ok := h.data[string(cmd.Args[1])]
	h.mu.RUnlock()

	if !ok {
		conn.WriteNull()
	} else {
		conn.WriteString(val.content)
	}
}

func (h *DB) del(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	ok := func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		data, err := h.data[string(cmd.Args[1])]
		data.content = ""
		h.data[string(cmd.Args[1])] = data
		return err
	}()

	if !ok {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (h *DB) set_exp(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		data, _ := h.data[string(cmd.Args[2])]
		data.ttl, _ = strconv.Atoi(string(cmd.Args[3]))
		h.wg.Add(1)
		go h.exp_key(string(cmd.Args[2]), data.ttl)
	}()

	conn.WriteString("OK")
}

func (h *DB) exp_key(key string, t int) {
	defer h.wg.Done()
	time.Sleep(time.Duration(t) * time.Second)
	h.mu.Lock()
	defer h.mu.Unlock()
	data, _ := h.data[key]
	data.content = ""
	data.ttl = -1
}
