package redis

import (
    "time"
)

const (
    CLUSTER_SLOTS = 16384
)

type redisCluster struct {
    slots	[CLUSTER_SLOTS]*redisNode
    nodes	map[string]*redisNode

    timeout	time.Duration
    keepAlive	int
    aliveTime	time.Duration
}


func New(addrs []string,  timeout time.Duration,
	keepAlive int, aliveTime time.Duration) (RedisCluster, error) {
    cluster := &redisCluster{
	nodes: make(map[string]*redisNode),
	timeout: timeout,
	keepAlive: keepAlive,
	aliveTime: aliveTime,
    }

    for i := 0; i < len(addrs); i++ {
	node := &redisNode{
	    address: addrs[i],
	    slaves: make([]*redisNode, 0),
	    timeout: timeout,
	    keepAlive: keepAlive,
	    aliveTime: aliveTime,
	}

	cluster.nodes[addrs[i]] = node
    }

    return cluster, nil
}
