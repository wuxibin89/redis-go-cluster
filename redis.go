package redis

type RedisCluster interface {
    Do(cmd string, args ...interface{}) (interface{}, error)
}
