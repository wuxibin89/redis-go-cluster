package main

import (
    "log"
    "fmt"
    "time"

    "github.com/chasex/redis-go-cluster"
)

func main() {
    cluster, err := redis.NewCluster(
	&redis.Options{
	    StartNodes: []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"},
	    ConnTimeout: 50 * time.Millisecond,
	    ReadTimeout: 50 * time.Millisecond,
	    WriteTimeout: 50 * time.Millisecond,
	    KeepAlive: 16,
	    AliveTime: 60 * time.Second,
	})

    if err != nil {
	log.Fatalf("redis.New error: %s", err.Error())
    }

    _, err = cluster.Do("set", "{user000}.name", "Joel")
    _, err = cluster.Do("set", "{user000}.age", "26")
    _, err = cluster.Do("set", "{user000}.country", "China")

    name, err := redis.String(cluster.Do("get", "{user000}.name"))
    if err != nil {
	log.Fatal(err)
    }
    age, err := redis.Int(cluster.Do("get", "{user000}.age"))
    if err != nil {
	log.Fatal(err)
    }
    country, err := redis.String(cluster.Do("get", "{user000}.country"))
    if err != nil {
	log.Fatal(err)
    }

    fmt.Printf("name: %s, age: %d, country: %s\n", name, age, country)

    cluster.Close()
    _, err = cluster.Do("set", "foo", "bar")
    if err == nil {
	log.Fatal("expect a none nil error")
    }
    log.Println(err)
}
