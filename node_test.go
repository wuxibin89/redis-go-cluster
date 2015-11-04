package redis

import (
    "time"
    //"reflect"
    "testing"
)

func TestRedisConn(t *testing.T) {
    node := &redisNode{
	address: "127.0.0.1:6379",
	timeout: 50 * time.Millisecond,
	keepAlive: 3,
	aliveTime: 30 * time.Second,
    }

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
}

func TestRedisDo(t *testing.T) {
    node := &redisNode{
	address: "127.0.0.1:6379",
	timeout: 200 * time.Millisecond,
	keepAlive: 3,
	aliveTime: 30 * time.Second,
    }

    reply, err := node.do("SET", "foo", "bar")
    if err != nil {
	t.Errorf("SET error: %s\n", err.Error())
    }
    if reply.(string) != "OK" {
	t.Errorf("unexpected value %v\n", reply)
    }

    reply, err = node.do("GET", "foo")
    if err != nil {
	t.Errorf("GET error: %s\n", err.Error())
    }
    if string(reply.([]byte)) != "bar" {
	t.Errorf("unexpected value %v\n", reply)
    }

    reply, err = node.do("GET", "notexist")
    if err != nil {
	t.Errorf("GET error: %s\n", err.Error())
    }
    if reply != nil {
	t.Errorf("unexpected value %v\n", reply)
    }
}
