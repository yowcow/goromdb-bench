package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

var milli = math.Pow(float64(10), float64(6))

func main() {
	var host string
	var addr string
	var file string
	var workers int
	var help bool
	flag.StringVar(&host, "host", "", "goromdb host")
	flag.StringVar(&addr, "addr", ":11211", "goromdb port")
	flag.StringVar(&file, "file", "keys.csv", "file to read")
	flag.IntVar(&workers, "workers", 1, "number of concurrent connections")
	flag.BoolVar(&help, "help", false, "help")
	flag.Parse()

	if host == "" || help {
		flag.Usage()
		os.Exit(1)
	}

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	in := make(chan string)
	ctx, cancel := context.WithCancel(context.Background())
	wg := start(ctx, host, addr, in, workers)

	count := 0
	r := csv.NewReader(f)
	for {
		rec, err := r.Read()
		if err != nil {
			break
		}
		in <- rec[0]
		count++
	}

	cancel()
	wg.Wait()
	close(in)

	fmt.Printf("=> all workers finished: %d\n", count)
}

func start(ctx context.Context, host, addr string, in <-chan string, workers int) *sync.WaitGroup {
	wg := new(sync.WaitGroup)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go work(ctx, i+1, host, addr, in, wg)
	}
	return wg
}

func work(ctx context.Context, id int, host, addr string, in <-chan string, wg *sync.WaitGroup) {
	count := 0
	tmtotal := float64(0)
	defer func() {
		if count > 0 {
			fmt.Printf("-> [worker %d] Avg resp time: %f ms (total count: %d)\n", id, tmtotal/float64(count), count)
		} else {
			fmt.Printf("-> [worker %d] Avg resp time: -- ms (total count: %d)\n", id, count)
		}
	}()
	defer wg.Done()

	mc := memcache.New(host + addr)
	tc := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tc.C:
			fmt.Printf("-> [worker %d] Avg resp time: %f ms (current count: %d)\n", id, tmtotal/float64(count), count)
		case url := <-in:
			tm0 := time.Now().UnixNano()
			mc.Get(url)
			tm1 := time.Now().UnixNano()
			tmtotal += float64(tm1-tm0) / milli
			count++
		}
	}
}
