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

package redis_test

import (
	"fmt"
	"github.com/chasex/redis-go-cluster"
	"time"
)

// Using SCAN on each node to retrieve a set of keys.
func ExampleCluster_DoEach_scan() {
	var err error
	var cluster *redis.Cluster
	allKeys := []string{}

	// initialise cluster
	cluster, err = redis.NewCluster(
		&redis.Options{
			StartNodes:   []string{"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"},
			ConnTimeout:  50 * time.Millisecond,
			ReadTimeout:  50 * time.Millisecond,
			WriteTimeout: 50 * time.Millisecond,
			KeepAlive:    16,
			AliveTime:    60 * time.Second,
		},
	)
	// add some values for this example
	_, _ = cluster.Do("MSET", "mykey:1", "a", "mykey:2", "b", "mykey:3", "c")

	// DoEach will execute the SCAN command on each node
	rawResults, _ := redis.Values(cluster.DoEach("SCAN", 0, "MATCH", "mykey:*"))

	// Loop through the results to retrieve the keys
	for _, rawResult := range rawResults {
		nodeValues, _ := redis.Values(rawResult, err)
		keys, _ := redis.Strings(nodeValues[1], err)
		for _, key := range keys {
			allKeys = append(allKeys, key)
		}
	}
	fmt.Println(allKeys)
	// Output: [mykey:1 mykey:2 mykey:3]
}
