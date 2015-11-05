package redis

import (
    "fmt"
    "time"
    "sync"
    "strings"
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

    mutex	sync.Mutex
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

    for _, node := range cluster.nodes {
	nodesInfo, err := Bytes(node.do("CLUSTER", "NODES"))
	if err != nil {
	    continue
	}
	if err := cluster.parseNodesInfo(nodesInfo); err != nil {
	    return cluster, nil
	}
    }

    return nil, fmt.Errorf("no invalid node in %v", addrs)
}

const (
    FIELD_NAME = iota
    FIELD_ADDR
    FIELD_FLAG
    FIELD_MASTER
    FIELD_PING
    FIELD_PONG
    FIELD_EPOCH
    FIELD_STATE
    FIELD_SLOT
    FIELD_EXTRA
)

func (cluster *redisCluster) parseNodesInfo(info []byte) error {
    infos := strings.Split(info, "\n")
    nodes := make([][]string, len(infos))

    for i := range infos {
	nodes[i] := strings.Split(infos[i])
	if len(nodes[i]) < FIELD_EXTRA {
	    return fmt.Errorf("missing field: %s", infos[i])
	}

	if nodes[FIELD_MASTER]
    }
}
