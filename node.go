package redis

import (
    "fmt"
    "net"
    "sync"
    "time"
    "bufio"
    "strconv"
    "container/list"
)

type redisConn struct {
    c	net.Conn
    t	time.Time

    br	*bufio.Reader
    bw	*bufio.Writer

    // Scratch space for formatting argument length.
    // '*' or '$', length, "\r\n"
    lenScratch [32]byte

    // Scratch space for formatting integers and floats.
    numScratch [40]byte
}

type redisNode struct {
    address	string
    slots	[CLUSTER_SLOTS/8]uint8
    numSlots	uint16

    slaves	[]*redisNode
    master	*redisNode

    conns	list.List
    timeout	time.Duration
    keepAlive	int
    aliveTime	time.Duration

    mutex	sync.Mutex
}

func (node *redisNode) getConn() (*redisConn, error) {
    node.mutex.Lock()

    // remove stale connections
    if node.timeout > 0 {
	for {
	    elem := node.conns.Back()
	    if elem == nil {
		break
	    }
	    conn := elem.Value.(*redisConn)
	    if conn.t.Add(node.timeout).After(time.Now()) {
		break
	    }
	    node.conns.Remove(elem)
	}
    }

    if node.conns.Len() <= 0 {
	node.mutex.Unlock()

	c, err := net.DialTimeout("tcp", node.address, node.timeout)
	if err != nil {
	    return nil, err
	}

	conn := &redisConn{
	    c: c,
	    br: bufio.NewReader(c),
	    bw: bufio.NewWriter(c),
	}

	return conn, nil
    }

    elem := node.conns.Back()
    node.conns.Remove(elem)
    return elem.Value.(*redisConn), nil
}

func (node *redisNode) releaseConn(conn *redisConn) {
    node.mutex.Lock()
    defer node.mutex.Unlock()

    if node.conns.Len() >= node.keepAlive || node.aliveTime <= 0 {
	return
    }

    conn.t = time.Now()
    node.conns.PushFront(conn)
}

func (node *redisNode) do(command string, args ...interface{}) (interface{}, error) {
    conn, err := node.getConn()
    if err != nil {
	return nil, err
    }

    if node.timeout > 0 {
	conn.c.SetDeadline(time.Now().Add(node.timeout))
    }

    if err = conn.writeCommand(command, args); err != nil {
	conn.shutdown()
	return nil, err
    }

    if reply, err := conn.readReply(); err != nil {
	conn.shutdown()
	return nil, err
    }

    node.releaseConn(conn)

    return reply, nil
}

func (conn *redisConn) shutdown() {
    conn.c.Close()
}

func (c *redisConn) writeLen(prefix byte, n int) error {
    c.lenScratch[len(c.lenScratch)-1] = '\n'
    c.lenScratch[len(c.lenScratch)-2] = '\r'
    i := len(c.lenScratch) - 3
    for {
        c.lenScratch[i] = byte('0' + n % 10)
        i -= 1
        n = n / 10
        if n == 0 {
	    break
        }
    }
    c.lenScratch[i] = prefix
    _, err := c.bw.Write(c.lenScratch[i:])
    return err
}

func (c *redisConn) writeString(s string) error {
    c.writeLen('$', len(s))
    c.bw.WriteString(s)
    _, err := c.bw.WriteString("\r\n")
    return err
}

func (c *redisConn) writeBytes(p []byte) error {
    c.writeLen('$', len(p))
    c.bw.Write(p)
    _, err := c.bw.WriteString("\r\n")
    return err
}

func (c *redisConn) writeInt64(n int64) error {
    return c.writeBytes(strconv.AppendInt(c.numScratch[:0], n, 10))
}

func (c *redisConn) writeFloat64(n float64) error {
    return c.writeBytes(strconv.AppendFloat(c.numScratch[:0], n, 'g', -1, 64))
}

func (c *redisConn) writeCommand(cmd string, args []interface{}) error {
    c.writeLen('*', len(args) + 1)
    err := c.writeString(cmd)

    for _, arg := range args {
	if err != nil {
		break
	}
	switch arg := arg.(type) {
	    case int64:
	        err = c.writeInt64(arg)
	    case float64:
	        err = c.writeFloat64(arg)
	    case string:
	        err = c.writeString(arg)
	    case []byte:
	        err = c.writeBytes(arg)
	    default:
	        err = fmt.Errorf("unknown type %T", arg)
	}
    }

    return err
}
