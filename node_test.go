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
	"testing"
	"time"
)

func TestRedisConn(t *testing.T) {
	node := newRedisNode()
	conn, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}
	node.releaseConn(conn)
	if node.conns.Len() != 1 {
		t.Errorf("releaseConn error")
	}

	conn1, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}
	if node.conns.Len() != 0 {
		t.Errorf("releaseConn error")
	}

	conn2, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}
	conn3, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}
	conn4, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}

	node.releaseConn(conn1)
	node.releaseConn(conn2)
	node.releaseConn(conn3)
	node.releaseConn(conn4)

	if node.conns.Len() != 3 {
		t.Errorf("releaseConn error")
	}

	conn, err = node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}

	if node.conns.Len() != 2 {
		t.Errorf("releaseConn error")
	}
}

func TestRedisDo(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("FLUSHALL")

	reply, err := node.do("SET", "foo", "bar")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if value, ok := reply.(string); !ok || value != "OK" {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("GET", "foo")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.([]byte); !ok || string(value) != "bar" {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("GET", "notexist")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if reply != nil {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("SETEX", "hello", 10, "world")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.(string); !ok || value != "OK" {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("INVALIDCOMMAND", "foo", "bar")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if _, ok := reply.(redisError); !ok {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HGETALL", "foo")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if _, ok := reply.(redisError); !ok {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HMSET", "myhash", "field1", "hello", "field2", "world")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.(string); !ok || value != "OK" {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HSET", "myhash", "field3", "nice")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.(int64); !ok || value != 1 {
		t.Errorf("unexpected value %v\n", reply)
	}

	reply, err = node.do("HGETALL", "myhash")
	if err != nil {
		t.Errorf("GET error: %s\n", err.Error())
	}
	if value, ok := reply.([]interface{}); !ok || len(value) != 6 {
		t.Errorf("unexpected value %v\n", reply)
	}
}

func TestRedisPipeline(t *testing.T) {
	node := newRedisNode()
	conn, err := node.getConn()
	if err != nil {
		t.Errorf("getConn error: %s\n", err.Error())
	}

	err = conn.send("PING")
	if err != nil {
		t.Errorf("send error: %s\n", err.Error())
	}
	err = conn.send("PING")
	if err != nil {
		t.Errorf("send error: %s\n", err.Error())
	}
	err = conn.send("PING")
	if err != nil {
		t.Errorf("send error: %s\n", err.Error())
	}

	err = conn.flush()
	if err != nil {
		t.Errorf("flush error: %s\n", err.Error())
	}

	reply, err := String(conn.receive())
	if err != nil {
		t.Errorf("flush error: %s\n", err.Error())
	}
	if reply != "PONG" {
		t.Errorf("receive error: %s", reply)
	}
	reply, err = String(conn.receive())
	if err != nil {
		t.Errorf("receive error: %s\n", err.Error())
	}
	if reply != "PONG" {
		t.Errorf("receive error: %s", reply)
	}
	reply, err = String(conn.receive())
	if err != nil {
		t.Errorf("receive error: %s\n", err.Error())
	}
	if reply != "PONG" {
		t.Errorf("receive error: %s", reply)
	}
	reply, err = String(conn.receive())
	if err == nil {
		t.Errorf("expect an error here")
	}
	if err.Error() != "no more pending reply" {
		t.Errorf("unexpected error: %s\n", err.Error())
	}

	conn.send("SET", "mycount", 100)
	conn.send("INCR", "mycount")
	conn.send("INCRBY", "mycount", 20)
	conn.send("INCRBY", "mycount", 20)

	conn.flush()

	conn.receive()
	conn.receive()
	conn.receive()
	value, err := Int(conn.receive())
	if value != 141 {
		t.Errorf("unexpected error: %d\n", reply)
	}
}

func newRedisNode() *redisNode {
	return &redisNode{
		address:      "127.0.0.1:6379",
		keepAlive:    3,
		aliveTime:    60 * time.Second,
		connTimeout:  50 * time.Millisecond,
		readTimeout:  50 * time.Millisecond,
		writeTimeout: 50 * time.Millisecond,
	}
}
