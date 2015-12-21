// Copyright 2015 Joel Wu
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
    "log"
    "fmt"
    "time"
    "sync"
    "strings"
    "strconv"
    "errors"
)

const (
    kClusterSlots	= 16384

    kRespOK		= 0
    kRespMove		= 1
    kRespAsk		= 2
    kRespConnTimeout	= 3
    kRespError		= 4
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

    rwLock	sync.RWMutex

    closed	bool
}

type nodeCommand struct {
    cmd	    string
    args    []interface{}
    reply   interface{}
    err	    error
}

type nodeBatch struct {
    node    *redisNode
    cmds    []nodeCommand

    err	    error
    done    chan int
}

type redisBatch struct {
    cluster	*redisCluster
    batches	[]nodeBatch
    index	[]int
}

// Put implement the Batch Put method.
func (batch *redisBatch) Put(cmd string, args ...interface{}) error {
    if len(args) < 1 {
	return errors.New("no key found in args")
    }

    key, err := toBytes(args[0])
    if err != nil {
	return err
    }

    slot := hashSlot(key)
    batch.cluster.rwLock.RLock()
    node := batch.cluster.slots[slot]
    batch.cluster.rwLock.RUnlock()

    if node == nil {
	return fmt.Errorf("no node serve slot %d for key %s", slot, key)
    }

    var i int
    for i = 0; i < len(batch.batches); i++ {
	if batch.batches[i].node == node {
	    batch.batches[i].cmds = append(batch.batches[i].cmds,
		nodeCommand{cmd: cmd, args: args})

	    batch.index = append(batch.index, i)
	    break
	}
    }

    if i == len(batch.batches) {
	batch.batches = append(batch.batches,
	    nodeBatch{
		node: node,
		cmds: []nodeCommand{{cmd: cmd, args: args}},
		done: make(chan int)})
	batch.index = append(batch.index, i)
    }

    return nil
}

// NewBatch implement the Cluster NewBatch method.
func (cluster *redisCluster) NewBatch() Batch {
    return &redisBatch{
	cluster: cluster,
	batches: make([]nodeBatch, 0),
	index: make([]int, 0),
    }
}

// RunBatch implement the Cluster RunBatch method.
func (cluster *redisCluster) RunBatch(batch Batch) ([]interface{}, error) {
    bat := batch.(*redisBatch)

    for i := range bat.batches {
	go doBatch(bat.batches[i])
    }

    for i := range bat.batches {
	<-bat.batches[i].done
    }

    var replies []interface{}
    for _, i := range bat.index {
	if bat.batches[i].err != nil {
	    return nil, bat.batches[i].err
	}

	replies = append(replies, bat.batches[i].cmds[0].reply)
	bat.batches[i].cmds = bat.batches[i].cmds[1:]
    }

    return replies, nil
}

func doBatch(batch nodeBatch) {
    conn, err := batch.node.getConn()
    if err != nil {
	batch.err = err
	batch.done <- 1
	return
    }

    for i := range batch.cmds {
	conn.send(batch.cmds[i].cmd, batch.cmds[i].args...)
    }

    err = conn.flush()
    if err != nil {
	batch.err = err
	conn.shutdown()
	batch.done <- 1
	return
    }

    for i := range batch.cmds {
	reply, err := conn.receive()
	if err != nil {
	    batch.err = err
	    conn.shutdown()
	    batch.done <- 1
	    return
	}

	batch.cmds[i].reply, batch.cmds[i].err = reply, err
    }

    batch.node.releaseConn(conn)
    batch.done <- 1
}

func newCluster(options *Options) (Cluster, error) {
    cluster := &redisCluster{
	connTimeout: options.ConnTimeout,
	readTimeout: options.ReadTimeout,
	writeTimeout: options.WriteTimeout,
	keepAlive: options.KeepAlive,
	aliveTime: options.AliveTime,
	updateList: make(chan updateMesg),
    }

    for i := range options.StartNodes {
	node := &redisNode{
	    address: options.StartNodes[i],
	    connTimeout: options.ConnTimeout,
	    readTimeout: options.ReadTimeout,
	    writeTimeout: options.WriteTimeout,
	    keepAlive: options.KeepAlive,
	    aliveTime: options.AliveTime,
	}

	err := cluster.update(node)
	if err != nil {
	    continue
	} else {
	    go cluster.handleUpdate()
	    return cluster, nil
	}
    }

    return nil, fmt.Errorf("no invalid node in %v", options.StartNodes)
}

// Do implement the Cluster Do method.
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

    cluster.rwLock.RLock()
    node := cluster.slots[slot]
    cluster.rwLock.RUnlock()

    if node == nil {
	return nil, fmt.Errorf("no node serve slot %d for key %s", slot, key)
    }

    reply, err := node.do(cmd, args...)
    if err != nil {
	return reply, err
    }

    resp := checkReply(reply)

    switch(resp) {
    case kRespOK, kRespError:
	return reply, err
    case kRespMove:
	return cluster.handleMove(node, reply.(redisError).Error(), cmd, args)
    case kRespAsk:
	return cluster.handleAsk(node, reply.(redisError).Error(), cmd, args)
    case kRespConnTimeout:
	return cluster.handleConnTimeout(node, cmd, args)
    }

    panic("unreachable")
}

func (cluster *redisCluster) Close() {
    cluster.rwLock.Lock()
    defer cluster.rwLock.Unlock()

    cluster.closed = true
}

type multiTask struct {
    node    *redisNode
    slot    uint16

    cmd	    string
    args    []interface{}

    reply   interface{}
    replies []interface{}
    err	    error

    done    chan int
}

func (cluster *redisCluster) multiSet(cmd string, args ...interface{}) (interface{}, error) {
    if len(args) & 1 != 0 {
	return nil, fmt.Errorf("invalid args %v", args)
    }

    tasks := make([]*multiTask, 0)

    cluster.rwLock.RLock()
    for i := 0; i < len(args); i += 2 {
	key, err := toBytes(args[i])
	if err != nil {
	    cluster.rwLock.RUnlock()
	    return nil, fmt.Errorf("invalid key %v", args[i])
	}

	slot := hashSlot(key)

	var j int
	for j = 0; j < len(tasks); j++ {
	    if tasks[j].slot == slot {
		tasks[j].args = append(tasks[j].args, args[i])	    // key
		tasks[j].args = append(tasks[j].args, args[i+1])    // value

		break
	    }
	}

	if j == len(tasks) {
	    node := cluster.slots[slot]
	    if node == nil {
		cluster.rwLock.RUnlock()
		return nil, fmt.Errorf("no node serve slot %d for key %s", slot, key)
	    }

	    task := &multiTask{
		node: node,
		slot: slot,
		cmd: cmd,
		args: []interface{}{args[i], args[i+1]},
		done: make(chan int),
	    }
	    tasks = append(tasks, task)
	}
    }
    cluster.rwLock.RUnlock()

    for i := range tasks {
	go handleSetTask(tasks[i])
    }

    for i := range tasks {
	<-tasks[i].done
    }

    for i := range tasks {
	_, err := String(tasks[i].reply, tasks[i].err)
	if err != nil {
	    return nil, err
	}
    }

    return "OK", nil
}

func (cluster *redisCluster) multiGet(cmd string, args ...interface{}) (interface{}, error) {
    tasks := make([]*multiTask, 0)
    index := make([]*multiTask, len(args))

    cluster.rwLock.RLock()
    for i := 0; i < len(args); i++ {
	key, err := toBytes(args[i])
	if err != nil {
	    cluster.rwLock.RUnlock()
	    return nil, fmt.Errorf("invalid key %v", args[i])
	}

	slot := hashSlot(key)

	var j int
	for j = 0; j < len(tasks); j++ {
	    if tasks[j].slot == slot {
		tasks[j].args = append(tasks[j].args, args[i])	    // key
		index[i] = tasks[j]

		break
	    }
	}

	if j == len(tasks) {
	    node := cluster.slots[slot]
	    if node == nil {
		cluster.rwLock.RUnlock()
		return nil, fmt.Errorf("no node serve slot %d for key %s", slot, key)
	    }

	    task := &multiTask{
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
    cluster.rwLock.RUnlock()

    for i := range tasks {
	go handleGetTask(tasks[i])
    }

    for i := range tasks {
	<-tasks[i].done
    }

    reply := make([]interface{}, len(args))
    for i := range reply {
	if index[i].err != nil {
	    return nil, index[i].err
	}

	if len(index[i].replies) < 0 {
	    panic("unreachable")
	}

	reply[i] = index[i].replies[0]
	index[i].replies = index[i].replies[1:]
    }

    return reply, nil
}

func handleSetTask(task *multiTask) {
    task.reply, task.err = task.node.do(task.cmd, task.args...)
    task.done <- 1
}

func handleGetTask(task *multiTask) {
    task.replies, task.err = Values(task.node.do(task.cmd, task.args...))
    task.done <- 1
}

func (cluster *redisCluster) handleMove(node *redisNode, replyMsg, cmd string, args []interface{}) (interface{}, error) {
    fields := strings.Split(replyMsg, " ")
    if len(fields) != 3 {
	return nil, errors.New(replyMsg)
    }

    // cluster change, inform back routine to update
    cluster.inform(node)

    cluster.rwLock.RLock()
    newNode, ok := cluster.nodes[fields[2]]
    cluster.rwLock.RUnlock()

    if !ok {
	return nil, errors.New(replyMsg)
    }

    return newNode.do(cmd, args...)
}

func (cluster *redisCluster) handleAsk(node *redisNode, replyMsg, cmd string, args []interface{}) (interface{}, error) {
    fields := strings.Split(replyMsg, " ")
    if len(fields) != 3 {
	return nil, errors.New(replyMsg)
    }

    cluster.rwLock.RLock()
    newNode, ok := cluster.nodes[fields[2]]
    cluster.rwLock.RUnlock()

    if !ok {
	cluster.inform(node)
	return nil, errors.New(replyMsg)
    }

    conn, err := newNode.getConn()
    if err != nil {
	return nil, errors.New(replyMsg)
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
	return nil, errors.New(replyMsg)
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
    cluster.rwLock.RLock()
    for _, randomNode = range cluster.nodes {
	if randomNode.address != node.address {
	    break
	}
    }
    cluster.rwLock.RUnlock()

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
	return nil, errors.New("connection timeout")
    }

    // cluster change, inform back routine to update
    cluster.inform(randomNode)

    cluster.rwLock.RLock()
    newNode, ok := cluster.nodes[fields[2]]
    cluster.rwLock.RUnlock()

    if !ok {
	return nil, errors.New(errMsg)
    }

    return newNode.do(cmd, args...)
}

func checkReply(reply interface{}) int {
    if _, ok := reply.(redisError); !ok {
	return kRespOK
    }

    errMsg := reply.(redisError).Error()

    if len(errMsg) >= 3 && string(errMsg[:3]) == "ASK" {
	return kRespAsk
    }

    if len(errMsg) >= 5 && string(errMsg[:5]) == "MOVED" {
	return kRespMove
    }

    if len(errMsg) >= 12 && string(errMsg[:12]) == "ECONNTIMEOUT" {
	return kRespConnTimeout
    }

    return kRespError
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
    kFieldName = iota
    kFieldAddr
    kFieldFlag
    kFieldMaster
    kFieldPing
    kFieldPong
    kFieldEpoch
    kFieldState
    kFieldSlot
)

func (cluster *redisCluster) update(node *redisNode) error {
    info, err := Values(node.do("CLUSTER", "SLOTS"))
    if err != nil {
	return err
    }

    errFormat := fmt.Errorf("update: %s invalid response", node.address)

    var nslots int
    var slots map[string][]uint16

    for i := range info {
	m, err := Values(i, err)
	if len(m) < 3 {
	    return errFormat
	}

	start, err := Int(m[0], err)
	if err != nil {
	    return errFormat
	}

	end, err := Int(m[1], err)
	if err != nil {
	    return errFormat
	}

	s, err := Strings(m[2], err)
	if err != nil || len(s) != 2 {
	    return errFormat
	}
	addr := s[0] + ":" + s[1]

	slot, ok := slots[addr]
	if !ok {
	    slot = make([]uint16, 2)
	}

	nslots += end - start + 1

	slot = append(slot, uint16(start))
	slot = append(slot , uint16(end))

	slots[addr] = slot
    }

    // TODO: Is full coverage really needed?
    if nslots != kClusterSlots {
	return fmt.Errorf("update: %s slots not full covered", node.address)
    }

    cluster.rwLock.Lock()
    defer cluster.rwLock.Unlock()

    t := time.Now()

    for addr, slot := range slots {
	node, ok := cluster.nodes[addr]
	if !ok {
	    node = &redisNode {
		address: addr,
		connTimeout: cluster.connTimeout,
		readTimeout: cluster.readTimeout,
		writeTimeout: cluster.writeTimeout,
		keepAlive: cluster.keepAlive,
		aliveTime: cluster.aliveTime,
	    }
	} else {
	    // reset slots
	    for i := 0; i < kClusterSlots; i++ {
		node.slots[i] = 0
	    }
	    node.numSlots = 0
	}

	n := len(slot)
	for i := 0; i < n - 1; i += 2 {
	    start := slot[i]
	    end := slot[i+1]

	    for j := start; j <= end; j++ {
		node.setSlot(j)
	    }
	}

	node.updateTime = t
    }

    // shrink
    for _, node := range cluster.nodes {
	if node.updateTime != t {
	    node.shutdown()
	}
    }

    return nil
}

func (cluster *redisCluster) handleUpdate() {
    for {
	msg := <-cluster.updateList
	err := cluster.update(msg.node)
	if err != nil {
	    log.Printf("update cluster info error: %s\n", err.Error())
	}
    }
}

func (cluster *redisCluster) inform(node *redisNode) {
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
	return crc16(key) & (kClusterSlots-1)
    }

    for e = s+1; e < len(key); e++ {
	if key[e] == '}' {
	    break
	}
    }

    if e == len(key) || e == s+1 {
	return crc16(key) & (kClusterSlots-1)
    }

    return crc16(key[s+1:e]) & (kClusterSlots-1)
}

func init() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)
}
