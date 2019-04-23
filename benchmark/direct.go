package main

import (
	"database/sql/driver"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kshvakov/clickhouse"
	"github.com/kshvakov/clickhouse/lib/data"
)

func main() {
	const ddl = `
	CREATE TABLE benchmark_driver_direct (
		event_time        DateTime
		, int32_value     Int32
		, int64_value     Int64
		, str_value       String
		, lc_string_value LowCardinality(String)
		, array_int8      Array(Int8)
	) Engine MergeTree PARTITION BY toYYYYMM(event_time) ORDER BY (event_time)
	`
	const query = `
	INSERT INTO benchmark_driver_direct (
		event_time
		, int32_value
		, int64_value
		, str_value
		, lc_string_value
		, array_int8
	) VALUES (?, ?, ?, ?, ?, ?)
	`
	conn, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?debug=false")
	if err != nil {
		log.Fatal(err)
	}
	{
		conn.Begin()
		stmt, _ := conn.Prepare("DROP TABLE IF EXISTS benchmark_driver_direct")
		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}
		if err := conn.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	{
		conn.Begin()
		stmt, _ := conn.Prepare(ddl)
		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}
		if err := conn.Commit(); err != nil {
			log.Fatal(err)
		}
	}

	start := time.Now()

	{
		conn.Begin()
		conn.Prepare(query)
		block, err := conn.Block()
		if err != nil {
			log.Fatal(err)
		}

		var blocks []*data.Block

		for i := 0; i < 10; i++ {
			blocks = append(blocks, block.Copy())
		}

		var wg sync.WaitGroup
		wg.Add(len(blocks))

		for _, block := range blocks {
			go func(block *data.Block) {
				block.Reserve()
				for i2 := 0; i2 < 500000; i2++ {
					block.NumRows++
					block.WriteDateTime(0, time.Now())
					block.WriteInt32(1, int32(1))
					block.WriteInt64(2, int64(1))
					block.WriteString(3, "str_value")
					block.WriteString(4, "lc_string_value")
					block.WriteArray(5, []int8{1, 2, 3, 4, 5})
				}
				if err := conn.WriteBlock(block); err != nil {
					log.Fatal(err)
				}
				wg.Done()
			}(block)
		}

		wg.Wait()

		if err := conn.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(time.Since(start))
}
