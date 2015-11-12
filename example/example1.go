package main

import (
    "log"
    "fmt"
    "time"
    "strconv"

    "github.com/chasex/redis-go-cluster"
)

func main() {
    cluster, err := redis.NewDefaultCluster([]string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"})
    if err != nil {
	log.Fatalf("redis.New error: %s", err.Error())
    }

    prefix := "mykey"
    for i := 0; i < 100000; i++ {
	key := prefix + strconv.Itoa(i)

	_, err := cluster.Do("set", key, i*10)
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
	if value != i*10 {
	    fmt.Printf("-mismatch %s: %d\n", key, value)
	    time.Sleep(100 * time.Millisecond)
	    continue
	}
	fmt.Printf("+set %s\n", key)
	time.Sleep(100 * time.Millisecond)
    }
}
