package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
)

// test with redis-client
// test with net.Dial (external + internal)

func TestSetCommand(t *testing.T) {
	conn, err := net.Dial("tcp", "localhost:6380")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	var query []byte
	result := make([]byte, 1024)
	query = []byte("lpush my_list 100\r\n")
	sent_query(conn, query)
	receive(conn, &result)

	query = []byte("lget my_list 0\r\n")
	sent_query(conn, query)
	receive(conn, &result)

	res_type := parse_type(result)
	print_respone(result, res_type)
}

func parse_type(b []byte) string {
	switch b[0] {
	case '+':
		return "string"
	case '-':
		return "error"
	case ':':
		return "integer"
	case '$':
		return "bulkstring"
	case '%':
		return "map"
	}

	return "invalid"
}

func parse_string(b []byte, is_bulk int) string {
	result := strings.Split(string(b), "\r\n")
	return result[is_bulk]
}

func parse_int(b []byte) int64 {
	result, err := strconv.ParseInt((strings.Split(string(b), "\r\n")[0]), 10, 64)
	if err != nil {
		panic(err)
	}

	return result
}

func print_respone(b []byte, t string) {
	switch t {
	case "string":
		fmt.Printf("%s\n", parse_string(b[1:], 0))
	case "error":
		fmt.Printf("%s\n", parse_string(b[1:], 0))
	case "bulkstring":
		fmt.Printf("%s\n", parse_string(b[1:], 1))
	case "integer":
		fmt.Printf("%d\n", parse_int(b[1:]))
	}
}

func sent_query(conn net.Conn, query []byte) {
	num, err := conn.Write(query)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Sent %d bytes\n", num)
}

func receive(conn net.Conn, result *[]byte) {
	num, err := conn.Read(*result)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Received %d bytes\n", num)
}
