package redis

import (
    "log"
    "fmt"
    "time"
    "strings"
    "strconv"
)

const (
    CLUSTER_SLOTS	= 16384

    RESP_OK		= 0
    RESP_MOVE		= 1
    RESP_ASK		= 2
    RESP_CONN_TIMEOUT	= 3
    RESP_ERROR		= 4
)

type updateMesg struct {
    node*	redisNode
    movedTime	time.Time
}

type redisCluster struct {
    slots	[]*redisNode
    nodes	map[string]*redisNode

    connTimeout time.Duration
    readTimeout time.Duration
    writeTimeout time.Duration

    keepAlive	int
    aliveTime	time.Duration

    updateTime	time.Time
    updateList	chan updateMesg

    discardTime	time.Duration
}

func NewRedisCluster(options *RedisOptions) (RedisCluster, error) {
    cluster := &redisCluster{
	connTimeout: options.connTimeout,
	readTimeout: options.readTimeout,
	writeTimeout: options.writeTimeout,
	keepAlive: options.keepAlive,
	aliveTime: options.aliveTime,
	discardTime: options.discardTime,
	updateList: make(chan updateMesg),
    }

    for i := range options.startNodes {
	node := &redisNode{
	    address: options.startNodes[i],
	    connTimeout: options.connTimeout,
	    readTimeout: options.readTimeout,
	    writeTimeout: options.writeTimeout,
	    keepAlive: options.keepAlive,
	    aliveTime: options.aliveTime,
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

    return nil, fmt.Errorf("no invalid node in %v", options.startNodes)
}

// Do excute a redis command with random number arguments. First argument will
// be used as key to hash to a slot, so it only supports a subset of redis 
// commands.
///
// SUPPORTED: most commands of keys, strings, lists, sets, sorted sets, hashes.
// NOT SUPPORTED: scripts, transactions, clusters.
// 
// Particularly, MSET/MSETNX/MGET are supported using result aggregation. 
// To MSET/MSETNX, there's no atomicity gurantee that given keys are set at once.
// It's possible that some keys are set, while others not.
//
// See full redis command list: http://www.redis.io/commands
func (cluster *redisCluster) Do(cmd string, args ...interface{}) (interface{}, error) {
    if len(args) < 1 {
	return nil, fmt.Errorf("no key found in args")
    }

    if cmd == "MSET" || cmd == "MSETNX" {
	return cluster.multiSet(cmd, args...)
    }

    if cmd == "MGET" {
	return cluster.multiGet(cmd, args...)
    }

    key, err := toBytes(args[0])
    if err != nil {
	return nil, fmt.Errorf("invalid key %v", args[0])
    }

    slot := hashSlot(key)
    node := cluster.slots[slot]
    if node == nil {
	return nil, fmt.Errorf("no node serve slot %d for key %s", slot, key)
    }

    reply, err := node.do(cmd, args...)
    resp := checkReply(reply, err)

    switch(resp) {
    case RESP_OK, RESP_ERROR:
	return reply, err
    case RESP_MOVE:
	return cluster.handleMove(node, reply.(redisError).Error(), cmd, args)
    case RESP_ASK:
	return cluster.handleAsk(node, reply.(redisError).Error(), cmd, args)
    case RESP_CONN_TIMEOUT:
	return cluster.handleConnTimeout(node, cmd, args)
    }

    panic("unreachable")
}

type subTask struct {
    node    *redisNode
    slot    uint16

    cmd	    string
    args    []interface{}

    reply   []interface{}
    err	    error

    done    chan int
}

func (cluster *redisCluster) multiSet(cmd string, args ...interface{}) (interface{}, error) {
    if len(args) & 1 != 0 {
	return nil, fmt.Errorf("invalid args")
    }

    tasks := make([]*subTask, 0)
    index := make([]*subTask, len(args)>>1)

    for i := 0; i < len(args); i += 2 {
	key, err := toBytes(args[i])
	if err != nil {
	    return nil, fmt.Errorf("invalid key %v", args[i])
	}

	slot := hashSlot(key)

	var j int
	for j := 0; j < len(tasks); j++ {
	    if tasks[j].slot == slot {
		tasks[j].args = append(tasks[j].args, args[i])	    // key
		tasks[j].args = append(tasks[j].args, args[i+1])    // value
		index[i>>1] = tasks[j]

		break
	    }
	}

	if j == len(tasks) {
	    node := cluster.slots[slot]
	    if node == nil {
		return nil, fmt.Errorf("no node serve slot %d for key %s", slot, key)
	    }

	    task := &subTask{
		node: node,
		slot: slot,
		cmd: cmd,
		args: []interface{}{args[i], args[i+1]},
		done: make(chan int),
	    }
	    tasks = append(tasks, task)
	    index[i>>1] = tasks[j]
	}
    }

    for i := range tasks {
	fmt.Printf("task[%d]: %s %v\n", i, tasks[i].cmd, tasks[i].args)
	go func() {
	    tasks[i].reply, tasks[i].err = Values(tasks[i].node.do(tasks[i].cmd, tasks[i].args...))
	    tasks[i].done <- 1
	}()
    }

    for i := range tasks {
	<-tasks[i].done
    }

    reply := make([]interface{}, len(args)>>1)
    for i := range reply {
	if index[i].err != nil {
	    errMsg := index[i].err.Error()
	    if len(errMsg) >= 5 && string(errMsg[:5]) == "MOVED" {
		cluster.sendUpdateMesg(index[i].node)
		return nil, index[i].err
	    }

	    fmt.Printf("oops, error occur: %s\n", index[i].err.Error())

	    return nil, index[i].err
	}

	if len(index[i].reply) < 0 {
	    panic("unreachable")
	}

	reply = append(reply, index[i].reply[0])
	index[i].reply = index[i].reply[1:]
    }

    return reply, nil
}

func (cluster *redisCluster) multiGet(cmd string, args ...interface{}) (interface{}, error) {
    tasks := make([]*subTask, 0)
    index := make([]*subTask, len(args))

    for i := 0; i < len(args); i++ {
	key, err := toBytes(args[i])
	if err != nil {
	    return nil, fmt.Errorf("invalid key %v", args[i])
	}

	slot := hashSlot(key)

	var j int
	for j := 0; j < len(tasks); j++ {
	    if tasks[j].slot == slot {
		tasks[j].args = append(tasks[j].args, args[i])	    // key
		index[i] = tasks[j]

		break
	    }
	}

	if j == len(tasks) {
	    node := cluster.slots[slot]
	    if node == nil {
		return nil, fmt.Errorf("no node serve slot %d for key %s", slot, key)
	    }

	    task := &subTask{
		node: node,
		slot: slot,
		cmd: cmd,
		args: []interface{}{args[i]},
		done: make(chan int),
	    }
	    tasks = append(tasks, task)
	    index[i] = tasks[j]
	}
    }

    for i := range tasks {
	fmt.Printf("task[%d]: %s %v\n", i, tasks[i].cmd, tasks[i].args)
	go func() {
	    tasks[i].reply, tasks[i].err = Values(tasks[i].node.do(tasks[i].cmd, tasks[i].args...))
	    tasks[i].done <- 1
	}()
    }

    for i := range tasks {
	<-tasks[i].done
    }

    reply := make([]interface{}, len(args))
    for i := range reply {
	if index[i].err != nil {
	    errMsg := index[i].err.Error()
	    if len(errMsg) >= 5 && string(errMsg[:5]) == "MOVED" {
		cluster.sendUpdateMesg(index[i].node)
		return nil, index[i].err
	    }

	    fmt.Printf("oops, error occur: %s\n", index[i].err.Error())

	    return nil, index[i].err
	}

	if len(index[i].reply) < 0 {
	    panic("unreachable")
	}

	reply = append(reply, index[i].reply[0])
	index[i].reply = index[i].reply[1:]
    }

    return reply, nil
}

func (cluster *redisCluster) handleMove(node *redisNode, replyMsg, cmd string, args []interface{}) (interface{}, error) {
    fields := strings.Split(replyMsg, " ")
    if len(fields) != 3 {
	return nil, redisError(replyMsg)
    }

    newNode, ok := cluster.nodes[fields[2]]
    if !ok {
	cluster.sendUpdateMesg(node)
	return nil, redisError(replyMsg)
    }

    reply, err := newNode.do(cmd, args...)
    cluster.sendUpdateMesg(node)

    return reply, err
}

func (cluster *redisCluster) handleAsk(node *redisNode, replyMsg, cmd string, args []interface{}) (interface{}, error) {
    fields := strings.Split(replyMsg, " ")
    if len(fields) != 3 {
	return nil, redisError(replyMsg)
    }

    newNode, ok := cluster.nodes[fields[2]]
    if !ok {
	cluster.sendUpdateMesg(node)
	return nil, redisError(replyMsg)
    }

    conn, err := newNode.getConn()
    if err != nil {
	return nil, redisError(replyMsg)
    }

    conn.send("ASKING")
    conn.send(cmd, args...)

    err = conn.flush()
    if err != nil {
	conn.shutdown()
	return nil, err
    }

    re, err := String(conn.receive())
    if err != nil || re != "OK" {
	conn.shutdown()
	return nil, redisError(replyMsg)
    }

    reply, err := conn.receive()
    if err != nil {
	conn.shutdown()
	return nil, err
    }

    node.releaseConn(conn)

    return reply, err
}

func (cluster *redisCluster) handleConnTimeout(node *redisNode, cmd string, args []interface{}) (interface{}, error) {
    var randomNode *redisNode

    // choose a random node other than previous one
    for _, randomNode = range cluster.nodes {
	if randomNode.address != node.address {
	    break
	}
    }

    reply, err := randomNode.do(cmd, args...)
    if err != nil {
	return reply, err
    }

    if _, ok := reply.(redisError); !ok {
	return reply, err
    }

    // ignore replies other than MOVED
    errMsg := reply.(redisError).Error()
    if len(errMsg) < 5 || string(errMsg[:5]) != "MOVED" {
	return reply, err
    }

    // When MOVED received, we check wether move adress equal to 
    // previous one. If equal, then it's just an connection timeout 
    // error, return error and carry on. If not, then the master may 
    // down or unreachable, a new master has served the slot, request 
    // new master and update cluster info.
    // 
    // TODO: At worst case, it will request redis 3 times on a single 
    // command, will this be a problem?
    fields := strings.Split(errMsg, " ")
    if len(fields) != 3 {
	return reply, err
    }

    if fields[2] == node.address {
	return nil, fmt.Errorf("connection timeout")
    }

    newNode, ok := cluster.nodes[fields[2]]
    if !ok {
	cluster.sendUpdateMesg(randomNode)
	return nil, redisError(errMsg)
    }

    reply, err = newNode.do(cmd, args...)
    cluster.sendUpdateMesg(randomNode)

    return reply, err
}

func checkReply(reply interface{}, err error) int {
    if err != nil {
	return RESP_ERROR
    }

    if _, ok := reply.(redisError); !ok {
	return RESP_OK
    }

    errMsg := reply.(redisError).Error()

    if len(errMsg) >= 3 && string(errMsg[:3]) == "ASK" {
	return RESP_ASK
    }

    if len(errMsg) >= 5 && string(errMsg[:5]) == "MOVED" {
	return RESP_MOVE
    }

    if len(errMsg) >= 12 && string(errMsg[:12]) == "ECONNTIMEOUT" {
	return RESP_CONN_TIMEOUT
    }

    return RESP_ERROR
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
	    connTimeout: cluster.connTimeout,
	    readTimeout: cluster.readTimeout,
	    writeTimeout: cluster.writeTimeout,
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
    cluster.updateTime = time.Now()

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
	msg := <-cluster.updateList
	// use last update timestamp, moved timestamp and discard time 
	// to control cluster info's update frequency.
	if cluster.updateTime.Add(cluster.discardTime).After(msg.movedTime) {
	    continue
	}

	err := cluster.updateClustrInfo(msg.node)
	if err != nil {
	    log.Printf("update cluster info error: %s\n", err.Error())
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

func hashSlot(key string) uint16 {
    var s, e int
    for s = 0; s < len(key); s++ {
	if key[s] == '{' {
	    break
	}
    }

    if s == len(key) {
	return crc16(key) & (CLUSTER_SLOTS-1)
    }

    for e = s+1; e < len(key); e++ {
	if key[e] == '}' {
	    break
	}
    }

    if e == len(key) || e == s+1 {
	return crc16(key) & (CLUSTER_SLOTS-1)
    }

    return crc16(key[s+1:e]) & (CLUSTER_SLOTS-1)
}

func init() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)
}
