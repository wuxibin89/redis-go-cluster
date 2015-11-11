package redis

import (
    "time"
)

type RedisOptions struct {
    startNodes	    []string

    connTimeout	    time.Duration
    readTimeout	    time.Duration
    writeTimeout    time.Duration

    keepAlive	    int
    aliveTime	    time.Duration

    discardTime	    time.Duration
}

type RedisCluster interface {
    Do(cmd string, args ...interface{}) (interface{}, error)
}

func NewDefaultCluster(addrs []string) (RedisCluster, error) {
    return NewRedisCluster(&RedisOptions{
	startNodes: addrs,
	connTimeout: 50 * time.Millisecond,
	readTimeout: 50 * time.Millisecond,
	writeTimeout: 50 * time.Millisecond,
	keepAlive: 16,
	aliveTime: 60 * time.Second,
	discardTime: 5 * time.Second,
    })
}
