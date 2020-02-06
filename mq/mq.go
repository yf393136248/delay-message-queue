package mq

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	jobChan chan *Job
	_currentNode *Node
	rwMutex sync.RWMutex
)

type CallbackFunc func(job *Job)

type ResolveTcpConnFunc func(conn *net.TCPConn)

type Node struct {
	Id int32
	Next *Node
	Jobs map[int][]*Job
}

type Job struct {
	Circles int
	PlusNodeNum int
	Type string
	Script string
	Params []interface{}
	Callback CallbackFunc
}

func NewCirCleMq(len int) *Node{
	jobChan = make(chan *Job)
	node := &Node{Id:1, Next:new(Node), Jobs: make(map[int][]*Job, 0)}
	current := node
	for i := 1; i <= len; i++ {
		current.Id = int32(i)
		if i == len {
			current.Next = node
		}else{
			current.Next = &Node{Id:0, Jobs: make(map[int][]*Job, 0)}
			current = current.Next
		}
	}
	return node
}

func (n *Node) Run(port int, resolveTcpConnFunc ResolveTcpConnFunc){
	go n.serve(port, resolveTcpConnFunc)
	_currentNode =  n
	for {
		go n.consumeJobs(_currentNode.Id)
		time.Sleep(time.Second * 1)
		_currentNode = _currentNode.Next
	}
}

func (n *Node) serve(port int, resolveTcpConnFunc ResolveTcpConnFunc) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:" + strconv.Itoa(port))
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
					go job.Callback(job)
				}
			}
		}
		ctx, _ := context.WithTimeout(context.Background(), time.Second * 1)
		n.push2Node(ctx, id)
		break
	}
}



func (n *Node) push2Node(ctx context.Context, id int32) {
	select {
	case job := <- jobChan:
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
	case <- ctx.Done():
		return
	}

}
