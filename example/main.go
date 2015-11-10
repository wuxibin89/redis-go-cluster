package main

import (
    "log"
    "fmt"
    "time"
    "strconv"

    "github.com/Chasex/redis-go-cluster"
)

func main() {
    cluster, err := redis.New([]string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"},
	100 * time.Millisecond, 16, 60 * time.Second, 1 * time.Second, 60 * time.Second)
    if err != nil {
	log.Fatalf("redis.New error: %s", err.Error())
    }

    prefix := "mykey"
    for i := 0; i < 100000; i++ {
	key := prefix + strconv.Itoa(i)

	_, err := cluster.Do("set", key, i)
	if err != nil {
	    fmt.Printf("-set %s: %s\n", key, err.Error())
	    time.Sleep(100 * time.Millisecond)
	    continue
	}
	value, err := redis.Int(cluster.Do("GET", key))
	if err != nil {
	    fmt.Printf("-get %s: %s\n", key, err.Error())
	    time.Sleep(100 * time.Millisecond)
	    continue
	}
	if value != i {
	    fmt.Printf("-mismatch %s: %d\n", key, value)
	    time.Sleep(100 * time.Millisecond)
	    continue
	}
	fmt.Printf("+set %s\n", key)
	time.Sleep(100 * time.Millisecond)
    }
}
