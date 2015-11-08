package redis

import (
    "fmt"
    "time"
    "strings"
    "strconv"
)

const (
    CLUSTER_SLOTS = 16384
)

type updateMesg struct {
    address	string
    movedTime	time.Time
}

type redisCluster struct {
    slots	[CLUSTER_SLOTS]*redisNode
    nodes	map[string]*redisNode

    timeout	time.Duration
    keepAlive	int
    aliveTime	time.Duration

    updateTime	time.Time
    discardTime time.Duration
    updateChan	chan updateMesg
}


func New(addrs []string,  timeout time.Duration, keepAlive int,
    aliveTime time.Duration, discardTime time.Duration) (RedisCluster, error) {

    cluster := &redisCluster{
	nodes: make(map[string]*redisNode),
	timeout: timeout,
	keepAlive: keepAlive,
	aliveTime: aliveTime,
	discardTime: discardTime,
    }

    for i := 0; i < len(addrs); i++ {
	node := &redisNode{
	    address: addrs[i],
	    slaves: make([]*redisNode, 0),
	    timeout: timeout,
	    keepAlive: keepAlive,
	    aliveTime: aliveTime,
	    updateChan: make(chan updateMesg),
	}

	cluster.nodes[addrs[i]] = node
    }

    for _, node := range cluster.nodes {
	if err := cluster.updateClustrInfo(node); err != nil {
	    continue
	}

	// TODO: start goroutine for update
	return cluster, nil
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

func (cluster *redisCluster) updateClustrInfo(node *redisNode) error {
    info, err := String(node.do("CLUSTER", "NODES"))
    if err != nil {
	return err
    }

    infos := strings.Split(info, "\n")
    fields := make([][]string, len(infos))
    nodes := make(map[string]*redisNode)

    for i := range fields {
	fields[i] = strings.Split(infos[i], " ")
	if len(fields[i]) < FIELD_EXTRA {
	    return fmt.Errorf("missing field: %s", infos[i])
	}

	nodes[fields[i][FIELD_ADDR]] = &redisNode {
	    name: fields[i][FIELD_NAME],
	    address: fields[i][FIELD_ADDR],
	    slaves: make([]*redisNode, 0),
	    timeout: cluster.timeout,
	    keepAlive: cluster.keepAlive,
	    aliveTime: cluster.aliveTime,
	    updateChan: cluster.updateChan,
	}
    }

    for i := range fields {
	// handle master node
	if strings.Index(fields[i][FIELD_FLAG], "master") != -1 {
	    for j := range fields[i][FIELD_SLOT:] {
		// ignore additional importing and migrating slots
		if strings.IndexByte(fields[i][FIELD_SLOT+j], '[') != -1 {
		    break
		}

		if err := cluster.setSlot(fields[i][FIELD_ADDR],
		    fields[i][FIELD_SLOT+j]); err != nil {
		    return err
		}
	    }
	} else {
	    // handle slave node
	    slave := cluster.nodes[fields[i][FIELD_ADDR]]
	    for _, v := range nodes {
	        if v.name == fields[i][FIELD_NAME] {
		    v.addSlave(slave)
		    break
	        }
	    }
	}
    }

    cluster.nodes = nodes

    return nil
}

func (cluster *redisCluster) setSlot(address, slots string) error {
    node := cluster.nodes[address]
    n := strings.IndexByte(slots, '-')

    // single slot
    if n == -1 {
	slot, err := strconv.ParseUint(slots, 10, 16)
	if err != nil {
	    return err
	}
	node.setSlot(uint16(slot))
	cluster.slots[slot] = node

	return nil
    }

    // range slots
    startSlot, err := strconv.ParseUint(slots[:n], 10, 16)
    if err != nil {
	return err
    }
    endSlot, err := strconv.ParseUint(slots[n+1:], 10, 16)
    if err != nil {
	return err
    }

    for slot := startSlot; slot <= endSlot; slot++ {
	node.setSlot(uint16(slot))
	cluster.slots[slot] = node
    }

    return nil
}
