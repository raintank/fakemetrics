package main

import (
	"flag"
	"sync"

	"fmt"
	"net"
	"os"
	"time"
)

var (
	addr string
	wg   sync.WaitGroup
)

func init() {
	flag.StringVar(&addr, "addr", "localhost:2003", "address of carbon host")
}

func main() {
	flag.Parse()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	to := time.Now().Unix()
	from := to - 6*24*60*60

	wg.Add(8)
	go do("fakemetrics.raw.min", conn, from, to)
	go do("fakemetrics.raw.max", conn, from, to)
	go do("fakemetrics.raw.all", conn, from, to)
	go do("fakemetrics.raw.default", conn, from, to)
	go do("fakemetrics.agg.min", conn, from, to)
	go do("fakemetrics.agg.max", conn, from, to)
	go do("fakemetrics.agg.all", conn, from, to)
	go do("fakemetrics.agg.default", conn, from, to)
	wg.Wait()
}

func do(key string, conn net.Conn, from, to int64) {
	i := 0
	for ts := from; ts < to; ts++ {
		if ts%7200 == 0 {
			fmt.Println(key, ts-from, "/", to-from)
		}
		_, err := fmt.Fprintf(conn, "%s %d %d\n", key, i, ts)
		if err != nil {
			fmt.Println(err)
		}
		i++
	}
	wg.Done()
}
