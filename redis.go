package redis

import (
    "time"
)

type Options struct {
    startNodes	    []string

    connTimeout	    time.Duration
    readTimeout	    time.Duration
    writeTimeout    time.Duration

    keepAlive	    int
    aliveTime	    time.Duration

    discardTime	    time.Duration
}

type Cluster interface {
    Do(cmd string, args ...interface{}) (interface{}, error)
    NewBatch() Batch
    RunBatch(batch Batch) (interface{}, error)
}

type Batch interface {
    Put(cmd string, args ...interface{}) error
}

func NewDefaultCluster(addrs []string) (Cluster, error) {
    return NewCluster(&Options{
	startNodes: addrs,
	connTimeout: 50 * time.Millisecond,
	readTimeout: 50 * time.Millisecond,
	writeTimeout: 50 * time.Millisecond,
	keepAlive: 16,
	aliveTime: 60 * time.Second,
	discardTime: 5 * time.Second,
    })
}
