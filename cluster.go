package redis

import (
    "log"
    "fmt"
    "time"
    "strings"
    "strconv"
)

const (
    CLUSTER_SLOTS = 16384
)

type updateMesg struct {
    node*	redisNode
    movedTime	time.Time
}

type redisCluster struct {
    slots	[]*redisNode
    nodes	map[string]*redisNode

    timeout	time.Duration
    keepAlive	int
    aliveTime	time.Duration

    updateTime	time.Time
    discardTime time.Duration
    updateList	chan updateMesg
}

func New(addrs []string,  timeout time.Duration, keepAlive int,
    aliveTime time.Duration, discardTime time.Duration) (RedisCluster, error) {

    cluster := &redisCluster{
	timeout: timeout,
	keepAlive: keepAlive,
	aliveTime: aliveTime,
	discardTime: discardTime,
	updateList: make(chan updateMesg),
    }

    for i := range addrs {
	node := &redisNode{
	    address: addrs[i],
	    timeout: timeout,
	    keepAlive: keepAlive,
	    aliveTime: aliveTime,
	}

	err := cluster.updateClustrInfo(node)
	if err != nil {
	    log.Println(err)
	    continue
	} else {
	    go cluster.handleUpdateMesg()
	    return cluster, nil
	}
    }

    return nil, fmt.Errorf("no invalid node in %v", addrs)
}

func (cluster *redisCluster) Do(cmd string, args ...interface{}) (interface{}, error) {
    if len(args) < 1 {
	return nil, fmt.Errorf("no key found in args")
    }

    key, err := toBytes(args[0])
    if err != nil {
	return nil, fmt.Errorf("invalid key %v", args[0])
    }

    slot := crc16([]byte(key))
    node := cluster.slots[slot % CLUSTER_SLOTS]
    if node == nil {
	return nil, fmt.Errorf("no node serve slot %d for key %s", slot, key)
    }

    // handle MOVED redis reply
    reply, err := node.do(cmd, args...)
    if err == nil {
	return reply, err
    }

    errMsg := err.Error()
    if len(errMsg) < 5 || string(errMsg[:5]) == "MOVED" {
	return reply, err
    }

    // TODO: what if master down, slave become master?

    fields := strings.Split(errMsg, " ")
    if len(fields) != 3 {
	return nil, fmt.Errorf("unknown reply: %s", errMsg)
    }

    node, ok := cluster.nodes[fields[2]]
    if !ok {
	return nil, fmt.Errorf("no node %s found in MOVED", fields[2])
    }

    reply, err = node.do(cmd, args)

    // Every time when receiving a MOVED reply, send a update message to 
    // inform update routine to get newest cluster info.
    cluster.sendUpdateMesg(node)

    return reply, err
}

func toBytes(arg interface{}) (string, error) {
    switch arg := arg.(type) {
    case int:
	return strconv.Itoa(arg), nil
    case int64:
	return strconv.Itoa(int(arg)), nil
    case float64:
	return strconv.FormatFloat(arg, 'g', -1, 64), nil
    case string:
	return arg, nil
    case []byte:
	return string(arg), nil
    default:
	return "", fmt.Errorf("unknown type %T", arg)
    }
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
)

func (cluster *redisCluster) updateClustrInfo(node *redisNode) error {
    info, err := String(node.do("CLUSTER", "NODES"))
    if err != nil {
	return err
    }

    infos := strings.Split(strings.Trim(info, "\n"), "\n")
    fields := make([][]string, len(infos))

    // create brand new slots info and nodes info
    slots := make([]*redisNode, CLUSTER_SLOTS)
    nodes := make(map[string]*redisNode)

    for i := range fields {
	fields[i] = strings.Split(infos[i], " ")
	if len(fields[i]) < FIELD_SLOT {
	    return fmt.Errorf("missing field: %s [%d] [%d]", infos[i], len(fields[i]), FIELD_SLOT)
	}

	nodes[fields[i][FIELD_ADDR]] = &redisNode {
	    name: fields[i][FIELD_NAME],
	    address: fields[i][FIELD_ADDR],
	    slaves: make([]*redisNode, 0),
	    timeout: cluster.timeout,
	    keepAlive: cluster.keepAlive,
	    aliveTime: cluster.aliveTime,
	}
    }

    for i := range fields {
	// ignore disconnected nodes
	if fields[i][FIELD_STATE] == "disconnected" {
	    continue
	}

	// handle master node
	if fields[i][FIELD_FLAG] == "master" || fields[i][FIELD_FLAG] == "myself,master" {
	    for j := range fields[i][FIELD_SLOT:] {
		// ignore additional importing and migrating slots
		if strings.IndexByte(fields[i][FIELD_SLOT+j], '[') != -1 {
		    break
		}

		if err := setSlot(slots, nodes, fields[i][FIELD_ADDR],
		    fields[i][FIELD_SLOT+j]); err != nil {
		    return err
		}
	    }
	    continue
	}

	// handle slave node
	if fields[i][FIELD_FLAG] == "slave" || fields[i][FIELD_FLAG] == "myself,slave" {
	    slave := nodes[fields[i][FIELD_ADDR]]
	    for _, v := range nodes {
	        if v.name == fields[i][FIELD_NAME] {
		    v.addSlave(slave)
		    break
	        }
	    }
	    continue
	}

	// TODO: ignore other nodes?
    }

    // TODO: need sync primitive?
    cluster.slots = slots
    cluster.nodes = nodes

    return nil
}

func setSlot(slots []*redisNode, nodes map[string]*redisNode, address, field string) error {

    node := nodes[address]
    n := strings.IndexByte(field, '-')

    // single slot
    if n == -1 {
	slot, err := strconv.ParseUint(field, 10, 16)
	if err != nil {
	    return err
	}
	node.setSlot(uint16(slot))
	slots[slot] = node

	return nil
    }

    // range slots
    startSlot, err := strconv.ParseUint(field[:n], 10, 16)
    if err != nil {
	return err
    }
    endSlot, err := strconv.ParseUint(field[n+1:], 10, 16)
    if err != nil {
	return err
    }

    for slot := startSlot; slot <= endSlot; slot++ {
	node.setSlot(uint16(slot))
	slots[slot] = node
    }

    return nil
}

func (cluster *redisCluster) handleUpdateMesg() {
    for {
	select {
	case msg := <-cluster.updateList:
	    // use last update timestamp, moved timestamp and discard time 
	    // to control cluster info's update frequency.
	    if !cluster.updateTime.Add(cluster.discardTime).After(msg.movedTime) {
	        continue
	    }

	    err := cluster.updateClustrInfo(msg.node)
	    if err != nil {
	        log.Printf("update cluster info error: %s\n", err.Error())
	    }
	case <-time.After(60 * time.Second):
	    for _, v := range cluster.nodes {
		if err := cluster.updateClustrInfo(v); err != nil {
		    break
		}
	    }
	}
    }
}

func (cluster *redisCluster) sendUpdateMesg(node *redisNode) {
    mesg := updateMesg{
	node: node,
	movedTime: time.Now(),
    }

    select {
    case cluster.updateList <- mesg:
	// Push update message, no more to do.
    default:
	// Update channel full, just carry on.
    }
}

func init() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)
}
