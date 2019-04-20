package main

import (
	"database/sql"
	"flag"
	"log"
	"time"

	_ "github.com/kshvakov/clickhouse"
)

var (
	backlog       int
	maxBlockSize  int
	flushInterval int
	chDSN         string
)

func init() {
	flag.IntVar(&backlog, "backlog", 1000, "backlog size")
	flag.IntVar(&maxBlockSize, "max-block-size", 500, "max block size")
	flag.IntVar(&flushInterval, "flush-interval", 1, "flush interval seconds")
	flag.StringVar(&chDSN, "clickhouse-dsn", "tcp://127.0.0.1:9000?debug=false", "")
}
func main() {
	flag.Parse()
	conn, err := sql.Open("clickhouse", chDSN)
	if err != nil {
		log.Fatal(err)
	}
	worker := worker{
		conn:          conn,
		backlog:       make(chan event, backlog), // max events in-flight
		shutdown:      make(chan struct{}),
		done:          make(chan struct{}),
		maxBlockSize:  maxBlockSize,
		flushInterval: time.Duration(flushInterval) * time.Second,
	}
	worker.Run()
}
