package mq

import (
	"bufio"
	"bytes"
	"context"
	"delay-message-queue/util"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	jobChan      chan *Job
	_currentNode *Node
	rwMutex      sync.RWMutex
	durableFile	 string
	durableFileHandler *os.File
	log           *util.Log
)

type CallbackFunc func(*Job, *util.Log)

type ResolveTcpConnFunc func(*net.TCPConn)

type Node struct {
	Id            int32
	Next          *Node
	Jobs          map[int][]*Job
	CirCleSlotNum int
}

type Job struct {
	Circles     int
	PlusNodeNum int
	Type        string
	Script      string
	Params      []interface{}
	Callback    CallbackFunc
}

//队列添加的请求消息体
type Msg struct {
	Type     string
	Script   string
	Params   []interface{}
	Interval int32
}

func init() {
	if !flag.Parsed() {
		flag.StringVar(&durableFile, "dbFile", "./data/mq.mqdb", "持久化存储数据文件")
	}
}

func NewCirCleMq(len int, logFilePath string) (*Node, error) {
	var (
		n *Node
		err error
	)
	if n, err = Load(); err == nil {
		return n, err
	}
	jobChan = make(chan *Job)
	log, err = util.NewLogs(logFilePath)
	if err != nil {
		return nil, err
	}
	durableFileHandler, _ = os.OpenFile(durableFile, os.O_RDWR | os.O_CREATE, 0755)
	node := &Node{
		Id: 1,
		Next: new(Node),
		Jobs: make(map[int][]*Job, 0),
		CirCleSlotNum: len,
	}
	current := node
	for i := 1; i <= len; i++ {
		current.Id = int32(i)
		if i == len {
			current.Next = node
		} else {
			current.Next = &Node{Id: 0, Jobs: make(map[int][]*Job, 0)}
			current = current.Next
		}
	}
	return node, nil
}

//save mq struct by time.Tick loop
func (n *Node) SaveTick() {
	go func() {
		for {
			select {
			case <- time.Tick(time.Second * 2):
				if err := Save(*n); err != nil {
					log.Debug("aergaer" + err.Error())
				}
			}
		}
	}()
}

func (n *Node) Run(port int) {
	go n.serve(port, n.connResolve)
	_currentNode = n
	for {
		go n.consumeJobs(_currentNode.Id)
		time.Sleep(time.Second * 1)
		_currentNode = _currentNode.Next
	}
}

func (n *Node) serve(port int, resolveTcpConnFunc ResolveTcpConnFunc) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:"+strconv.Itoa(port))
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			fmt.Println("tcp accept error, reason:", err)
		}
		go resolveTcpConnFunc(tcpConn)
	}
}

func (n *Node) PushJob(job *Job) {
	jobChan <- job
}

func (n *Node) consumeJobs(id int32) {
	_node := n
	for {
		if _node.Id != id {
			_node = _node.Next
			continue
		}
		for circles := range _node.Jobs {
			_jobs := _node.Jobs[circles]
			delete(_node.Jobs, circles)
			if circles >= 1 {
				minute1Jobs, ok := _node.Jobs[circles-1]
				if ok {
					_node.Jobs[circles-1] = append(minute1Jobs, _jobs...)
				} else {
					_node.Jobs[circles-1] = _jobs
				}
			} else {
				for _, job := range _jobs {
					go job.Callback(job, log)
				}
			}
		}
		ctx, _ := context.WithTimeout(context.Background(), time.Second*1)
		n.push2Node(ctx, id)
		break
	}
}

func (n *Node) push2Node(ctx context.Context, id int32) {
	select {
	case job := <-jobChan:
		_node := n
		for {
			if _node.Id != id {
				_node = _node.Next
				continue
			}
			//非整数倍的圈数的，需要额外增加前进步数
			if job.PlusNodeNum > 0 {
				for ; job.PlusNodeNum > 0; job.PlusNodeNum-- {
					_node = _node.Next
				}
			}
			jobs, ok := _node.Jobs[job.Circles]
			if ok {
				_node.Jobs[job.Circles] = append(jobs, job)
			} else {
				_j := make([]*Job, 0)
				_j = append(_j, job)
				_node.Jobs[job.Circles] = _j
			}
			break
		}
	case <-ctx.Done():
		return
	}

}

func (n *Node) connResolve(conn *net.TCPConn) {
	var cb CallbackFunc
	defer conn.Close()
	fmt.Println(conn.RemoteAddr().String())
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil || err == io.EOF{
			if err == io.EOF {
				log.Debug("客户端断开连接")
				break
			}
			fmt.Println(err)
			conn.Write(n.connResponseFail())
			break
		}
		body := Msg{}
		if err := json.Unmarshal(msg, &body); err != nil {
			fmt.Println(err)
			conn.Write(n.connResponseFail())
			continue
		}
		if body.Type == "" {
			conn.Write(n.connResponseFail())
			continue
		}
		circleNum := int(body.Interval) / n.CirCleSlotNum
		switch body.Type {
		case "cmd":
			cb = jobCmdCallback
			break
		case "api":
			cb = jobApiCallback
		}
		job := &Job{
			Circles:     circleNum,
			PlusNodeNum: int(body.Interval) - circleNum*n.CirCleSlotNum,
			Type:        body.Type,
			Script:      body.Script,
			Params:      body.Params,
			Callback:    cb,
		}
		n.PushJob(job)
		conn.Write(n.connResponseSucc())
	}
}

func (n *Node) connResponseFail() []byte {
	return []byte(`{"code":400, "msg": "fail"}`)
}

func (n *Node) connResponseSucc() []byte {
	return []byte(`{"code":200, "msg": "succ"}`)
}

//Serialize the mq struct to local storage file
func Save(n Node) error {
	var data bytes.Buffer
	encoder := gob.NewEncoder(&data)
	if err := encoder.Encode(n); err != nil {
		return err
	}
	if _, err := durableFileHandler.Write(data.Bytes()); err != nil {
		return  err
	}
	return nil
}

//Load mq serialize struct from local storage file
func Load() (*Node, error) {
	var (
		data bytes.Buffer
		fileCnt []byte
		n *Node
	)
	_, err := os.Stat(durableFile)
	if err != nil {
		return nil, err
	}
	n = new(Node)
	fHandler, _ := os.OpenFile(durableFile, os.O_RDWR, 0755)
	fHandler.Read(fileCnt)
	data.Write(fileCnt)
	decoder := gob.NewDecoder(&data)
	if err := decoder.Decode(n); err != nil {
		return nil, err
	}
	return n, nil
}