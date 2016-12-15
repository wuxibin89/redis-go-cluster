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

// DoEach excutes a redis command with random number arguments on each of the nodes in the cluster.
//
// The key difference between Do and DoEach is that Do uses the key to calculate the slot and determine
// which node to call, whereas DoEach will unconditionally execute the command in all nodes of the cluster.
//
// Unlike Do, DoEach does not require the first argument to be a key (see example below).
// The output from this method is the aggregate of the results from each node.
//
// See full redis command list: http://www.redis.io/commands
func (cluster *Cluster) DoEach(cmd string, args ...interface{}) (interface{}, error) {
	tasks := make([]*multiTask, 0)

	cluster.rwLock.RLock()
	for _, node := range cluster.nodes {

		task := &multiTask{
			node: node,
			cmd:  cmd,
			args: args,
			done: make(chan int),
		}
		tasks = append(tasks, task)
	}
	cluster.rwLock.RUnlock()

	for i := range tasks {
		go handleEachTask(tasks[i])
	}

	for i := range tasks {
		<-tasks[i].done
	}

	reply := make([]interface{}, 0)
	for _, task := range tasks {
		if task.err != nil {
			return nil, task.err
		}
		reply = append(reply, task.replies)
	}

	return reply, nil
}

func handleEachTask(task *multiTask) {
	task.replies, task.err = Values(task.node.do(task.cmd, task.args...))
	task.done <- 1
}
