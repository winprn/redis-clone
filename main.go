package main

import (
	"log"

	"github.com/tidwall/redcon"
)

const addr = ":6380"

func main() {
	go log.Printf("Running on %s", addr)
	mux := redcon.NewServeMux()
	db := NewDB()
	ldb := NewListDB()

	mux.HandleFunc("lpush", ldb.lpush)
	mux.HandleFunc("rpush", ldb.rpush)
	mux.HandleFunc("lget", ldb.get)
	mux.HandleFunc("get", db.get)
	mux.HandleFunc("set", db.set)

	err := redcon.ListenAndServe(addr, mux.ServeRESP, func(conn redcon.Conn) bool {
		// accept all request
		return true
	}, func(conn redcon.Conn, err error) {
		log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
	})

	if err != nil {
		log.Fatal(err)
	}
	db.wg.Wait()
}
