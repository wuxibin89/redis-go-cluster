package main

import (
    "log"
    "fmt"

    "github.com/chasex/redis-go-cluster"
)

func main() {
    cluster, err := redis.NewDefaultCluster([]string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"})
    if err != nil {
	log.Fatalf("redis.New error: %s", err.Error())
    }

    batch := cluster.NewBatch()
    batch.Put("INCR", "mycount")
    batch.Put("INCR", "mycount")
    batch.Put("INCR", "mycount")

    reply, err := redis.Values(cluster.RunBatch(batch))
    if err != nil {
	log.Fatalf("RunBatch error: %s", err.Error())
    }

    for i := 0; i < 3; i++ {
	var resp int
	reply, err = redis.Scan(reply, &resp)
	if err != nil {
	    log.Fatalf("RunBatch error: %s", err.Error())
	}

	fmt.Printf("[%d] return: %d\n", i, resp)
    }

    batch = cluster.NewBatch()
    err = batch.Put("LPUSH", "country_list", "france")
    if err != nil {
	log.Fatalf("LPUSH error: %s", err.Error())
    }
    err = batch.Put("LPUSH", "country_list", "italy")
    if err != nil {
	log.Fatalf("LPUSH error: %s", err.Error())
    }
    err = batch.Put("LPUSH", "country_list", "germany")
    if err != nil {
	log.Fatalf("LPUSH error: %s", err.Error())
    }
    err = batch.Put("INCRBY", "countries", 3)
    if err != nil {
	log.Fatalf("INCRBY error: %s", err.Error())
    }
    err = batch.Put("LRANGE", "country_list", 0, -1)
    if err != nil {
	log.Fatalf("LRANGE error: %s", err.Error())
    }

    reply, err = redis.Values(cluster.RunBatch(batch))
    if err != nil {
	log.Fatalf("RunBatch error: %s", err.Error())
    }

    for i := 0; i < 4; i++ {
	var resp int
	reply, err = redis.Scan(reply, &resp)
	if err != nil {
	    log.Fatalf("RunBatch error: %s", err.Error())
	}

	fmt.Printf("[%d] return: %d\n", i, resp)
    }

    countries, err := redis.Strings(reply[0], nil)
    if err != nil {
	log.Fatalf("redis.Stgrings error: %s", err.Error())
    }

    for i := range countries {
	fmt.Printf("[%d] %s\n", i, countries[i])
    }
}
