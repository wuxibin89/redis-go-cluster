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
	"strconv"
	"testing"
)

func TestRedisInt(t *testing.T) {
	node := newRedisNode()
	_, err := node.do("SET", "count", 10)
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}

	count, err := Int(node.do("INCR", "count"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if count != 11 {
		t.Errorf("unexpected count %d", count)
	}

	count, err = Int(node.do("INCRBY", "count", 10))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if count != 21 {
		t.Errorf("unexpected count %d", count)
	}

	_, err = node.do("SET", "count", "string string")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}

	count, err = Int(node.do("GET", "count"))
	if err == nil {
		t.Errorf("expected an error")
	}
}

func TestRedisInt64(t *testing.T) {
	node := newRedisNode()

	count, err := Int64(node.do("LPUSH", "mylist", 10))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if count != 1 {
		t.Errorf("unexpected count %d", count)
	}

	count, err = Int64(node.do("LPUSH", "mylist", -20))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if count != 2 {
		t.Errorf("unexpected count %d", count)
	}

	value, err := Int64(node.do("RPOP", "mylist"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if value != 10 {
		t.Errorf("unexpected count %d", count)
	}

	value, err = Int64(node.do("RPOP", "mylist"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if value != -20 {
		t.Errorf("unexpected count %d", count)
	}
}

func TestRedisFloat64(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("SET", "pi", 3.14)
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}

	pi, err := Float64(node.do("GET", "pi"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if strconv.FormatFloat(pi, 'f', -1, 64) != "3.14" {
		t.Errorf("unexpected value: %f\n", pi)
	}
}

func TestRedisString(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("SET", "hello", "hello world")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}

	value, err := String(node.do("GET", "hello"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if value != "hello world" {
		t.Errorf("unexpected value: %f\n", value)
	}
}

func TestRedisBytes(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("SET", "mykey1", "hello world")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}

	value, err := Bytes(node.do("GET", "mykey1"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if string(value) != "hello world" {
		t.Errorf("unexpected value: %f\n", value)
	}
}

func TestRedisBool(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("SET", "mykey2", "true")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}

	value, err := Bool(node.do("GET", "mykey2"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if value != true {
		t.Errorf("unexpected value: %t\n", value)
	}

	_, err = node.do("SET", "mykey2", 0)
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}

	value, err = Bool(node.do("GET", "mykey2"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if value != false {
		t.Errorf("unexpected value: %t\n", value)
	}
}

func TestRedisInts(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("MSET", "key1", 0, "key2", 1, "key3", 2)
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	value, err := Ints(node.do("MGET", "key1", "key2", "key3"))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if len(value) != 3 || value[0] != 0 || value[1] != 1 || value[2] != 2 {
		t.Errorf("unexpected value: %v\n", value)
	}
}

func TestRedisStrings(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("LPUSH", "mylist2", "aaa")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	_, err = node.do("LPUSH", "mylist2", "bbb")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	_, err = node.do("LPUSH", "mylist2", "ccc")
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}

	value, err := Strings(node.do("LRANGE", "mylist2", 0, -1))
	if err != nil {
		t.Errorf("SET error: %s\n", err.Error())
	}
	if len(value) != 3 || string(value[0]) != "ccc" || string(value[1]) != "bbb" || string(value[2]) != "aaa" {
		t.Errorf("unexpected value: %v\n", value)
	}
}

func TestRedisStringMap(t *testing.T) {
	node := newRedisNode()

	_, err := node.do("HMSET", "hashkey1", "name", "joel", "age", "26", "gender", "male")
	if err != nil {
		t.Errorf("HMSET error: %s\n", err.Error())
	}

	value, err := StringMap(node.do("HGETALL", "hashkey1"))
	if err != nil {
		t.Errorf("HGETALL error: %s\n", err.Error())
	}
	if v, ok := value["name"]; !ok || v != "joel" {
		t.Errorf("unexpected value: %v\n", value)
	}
	if v, ok := value["age"]; !ok || v != "26" {
		t.Errorf("unexpected value: %v\n", value)
	}
	if v, ok := value["gender"]; !ok || v != "male" {
		t.Errorf("unexpected value: %v\n", value)
	}
}

func TestRedisScan(t *testing.T) {
	node := newRedisNode()

	kv := []string{"key1", "hello", "key2", "-1024", "key3", "false", "key4", "-3.14"}
	ikv := make([]interface{}, len(kv))
	for i := range ikv {
		ikv[i] = kv[i]
	}
	_, err := node.do("MSET", ikv...)
	if err != nil {
		t.Errorf("MSET error: %s\n", err.Error())
	}

	keys := []string{"key1", "key2", "key3", "key4"}
	ikeys := make([]interface{}, len(keys))
	for i := range ikeys {
		ikeys[i] = keys[i]
	}
	reply, err := Values(node.do("MGET", ikeys...))
	if err != nil {
		t.Errorf("MSET error: %s\n", err.Error())
	}

	var stringVal string
	var intVal int
	var boolVal bool
	var floatVal float64

	if reply, err = Scan(reply, &stringVal); err != nil {
		t.Errorf("Scan error: %s\n", err.Error())
	}
	if stringVal != "hello" {
		t.Errorf("unexpected value: %s\n", stringVal)
	}

	if reply, err = Scan(reply, &intVal); err != nil {
		t.Errorf("Scan error: %s\n", err.Error())
	}
	if intVal != -1024 {
		t.Errorf("unexpected value: %s\n", stringVal)
	}

	if reply, err = Scan(reply, &intVal); err == nil {
		t.Errorf("expected an error")
	}

	if reply, err = Scan(reply, &floatVal); err != nil {
		t.Errorf("Scan error: %s\n", err.Error())
	}

	reply, err = Values(node.do("MGET", ikeys...))
	if err != nil {
		t.Errorf("MSET error: %s\n", err.Error())
	}

	if reply, err = Scan(reply, &stringVal, &intVal, &boolVal, &floatVal); err != nil {
		t.Errorf("Scan error: %s\n", err.Error())
	}
	if stringVal != "hello" && intVal != -1024 && boolVal != false {
		t.Errorf("unexpected value: %s %d %t %f\n", stringVal, intVal, boolVal, floatVal)
	}
}
