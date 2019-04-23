package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/kshvakov/clickhouse"
)

func main() {
	const ddl = `
	CREATE TABLE benchmark_driver_native (
		event_time        DateTime
		, int32_value     Int32
		, int64_value     Int64
		, str_value       String
		, lc_string_value LowCardinality(String)
		, array_int8      Array(Int8)
	) Engine MergeTree PARTITION BY toYYYYMM(event_time) ORDER BY (event_time)
	`
	const query = `
	INSERT INTO benchmark_driver_native (
		event_time
		, int32_value
		, int64_value
		, str_value
		, lc_string_value
		, array_int8
	) VALUES (?, ?, ?, ?, ?, ?)
	`
	conn, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=false")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec("DROP TABLE IF EXISTS benchmark_driver_native"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(ddl); err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	for i := 0; i < 10; i++ {
		scope, err := conn.Begin()
		if err != nil {
			log.Fatal(err)
		}
		block, err := scope.Prepare(query)
		if err != nil {
			log.Fatal(err)
		}
		for i2 := 0; i2 < 500000; i2++ {
			if _, err := block.Exec(
				time.Now(),
				int32(1),
				int64(1),
				"str_value",
				"lc_string_value",
				[]int8{1, 2, 3, 4, 5},
			); err != nil {
				log.Fatal(err)
			}
		}
		if err := scope.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(time.Since(start))
}
