package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type event struct {
	value string
	time  time.Time
}

type worker struct {
	conn           *sql.DB
	backlog        chan event
	shutdown       chan struct{}
	done           chan struct{}
	maxBlockSize   int
	flushInterval  time.Duration
	retryLog       []event
	retryLogOffset int
}

func (wrk *worker) handle(e event) {
	wrk.backlog <- e
}

func (wrk *worker) next() (event, bool) {
	if len(wrk.retryLog) > wrk.retryLogOffset {
		wrk.retryLogOffset++
		return wrk.retryLog[wrk.retryLogOffset-1], true
	}
	select {
	case event := <-wrk.backlog:
		wrk.retryLogOffset++
		wrk.retryLog = append(wrk.retryLog, event)
		return event, true
	default:
		return event{}, false
	}
}

func (wrk *worker) background() {
	flush := time.NewTimer(wrk.flushInterval)
	for {
		select {
		case <-flush.C:
			switch err := wrk.flush(false); {
			case err != nil:
				flush.Reset(5 * wrk.flushInterval)
				{
					fmt.Println("flush error: ", err)
				}
			default:
				flush.Reset(wrk.flushInterval)
			}
		case <-wrk.shutdown:
			wrk.flush(true)
			{
				wrk.done <- struct{}{}
			}
		}
	}
}

func (wrk *worker) flush(all bool) error {
	fmt.Printf("flush. all=%t, backlog size=%d, retry log size=%d\n", all, len(wrk.backlog), len(wrk.retryLog))
	wrk.retryLogOffset = 0
	var (
		start  = time.Now()
		events []event
	)
	for {
		event, ok := wrk.next()
		if !ok {
			break
		}
		events = append(events, event)
		if !all && (len(events) >= wrk.maxBlockSize || time.Since(start) >= wrk.flushInterval) {
			break
		}
	}
	if len(events) != 0 {
		scope, err := wrk.conn.Begin() // pin connection
		if err != nil {
			return err
		}
		block, err := scope.Prepare("INSERT INTO test_go_worker (Time, Value) VALUES (?, ?)") // fetch metadata block from ClickHouse
		if err != nil {
			return err
		}
		for _, event := range events {
			if _, err := block.Exec(event.time, event.value); err != nil { // write data to the block
				return err
			}
		}
		if err := scope.Commit(); err != nil { // send block to the ClickHouse server and release connection
			return err
		}
		fmt.Println("flush events: ", len(events))
	}
	wrk.retryLog = wrk.retryLog[:0]
	return nil
}

func (wrk *worker) Run() {
	go wrk.background()
	var (
		signals    = make(chan os.Signal)
		subscriber = NewSubscriber(wrk.handle)
	)
	signal.Notify(signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	fmt.Printf("sig: [%s]\n", <-signals)

	subscriber.Unsubscribe()

	wrk.shutdown <- struct{}{}
	timeout := time.Tick(10 * time.Second)
	select {
	case <-timeout:
		fmt.Println("shutdown with timeout")
	case <-wrk.done:
		fmt.Println("graceful shutdown")
	}
	os.Exit(0)
}

// NewSubscriber создает новый генератор событий
// в реальных приложениях таким генератором служит клиент к: kafka, NATS, Rabbit MQ и т.д..
func NewSubscriber(handler func(event)) *eventGenerator {
	generator := eventGenerator{
		handler: handler,
		ticker:  time.NewTicker(10 * time.Millisecond),
	}
	go generator.background()
	return &generator
}

type eventGenerator struct {
	ticker  *time.Ticker
	handler func(event)
}

func (e *eventGenerator) background() {
	for {
		select {
		case t := <-e.ticker.C:
			if !t.IsZero() {
				e.handler(event{
					time:  t,
					value: fmt.Sprintf("value (%d)", t.Unix()),
				})
			}
		}
	}
}

func (e *eventGenerator) Unsubscribe() {
	e.ticker.Stop()
}
