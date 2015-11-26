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
    "time"
)

// Options is used to initialize a new redis cluster.
type Options struct {
    StartNodes	    []string	    // Startup nodes

    ConnTimeout	    time.Duration   // Connection timeout
    ReadTimeout	    time.Duration   // Read timeout
    WriteTimeout    time.Duration   // Write timeout

    KeepAlive	    int		    // Maximum keep alive connecion in each node
    AliveTime	    time.Duration   // Keep alive timeout
}

// Cluster is a redis client that manage connections to redis nodes, 
// cache and update cluster info, and execute all kinds of commands.
// Multiple goroutines may invoke methods on a cluster simutaneously.
type Cluster interface {
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
    // See README.md for more details.
    // See full redis command list: http://www.redis.io/commands
    Do(cmd string, args ...interface{}) (interface{}, error)

    // NewBatch create a new batch to pack mutiple commands.
    NewBatch() Batch

    // RunBatch execute commands in batch simutaneously. If multiple commands are 
    // directed to the same node, they will be merged and sent at once using pipeling.
    RunBatch(batch Batch) ([]interface{}, error)
}

// Batch pack multiple commands, which should be supported by Do method.
type Batch interface {
    // Put add a redis command to batch.
    Put(cmd string, args ...interface{}) error
}

// NewCluster create a new redis cluster client with specified options.
func NewCluster(options *Options) (Cluster, error) {
    return newCluster(options)
}
