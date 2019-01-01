package main

import (
	"bufio"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"io"
	"log"
	"net"
	"time"
)

type Msg struct {
	After   int			//多少秒后执行脚本
	Script 	string		//执行的脚本
	Params 	string		//执行的参数
}
type Circle struct {
	Pears		[]*Pear
	CurrentIdx  int	//当前循环位置索引
	CurrentPear *Pear
}

//circle的一个块儿
type Pear struct {
	Slots	[]Slot	//slot组
	Next	*Pear
	Index   int
}

type Slot struct {
	Script 	string	//脚本名称
	Params  string	//脚本参数
	CircleNum	int	//循环几圈后执行本slot内部的脚本
}

func (c *Circle) Run() {
	T := time.NewTicker(time.Second)
	for {
		select {
		case <-T.C:
			c.CurrentPear = c.CurrentPear.Next
			c.CurrentIdx  = c.CurrentPear.Index
			fmt.Println("运行...", c.CurrentIdx)
		}
	}
}

func (c *Circle) AddTask(slot *Slot) {
	fmt.Println("接收到新任务", c.CurrentIdx)
}

func NewCircle(num int) *Circle {
	if num < 2 {
		log.Fatal("环形队列的元素块数量不能小于2！")
	}
	pears := make([]*Pear, num)
	for i := 0; i < num; i++ {
		p := &Pear{
			Slots:  make([]Slot, 0),
			Next: 	nil,
			Index: 	i,
		}
		pears[i] = p
		if i > 0 {
			pears[i - 1].Next = p
		}
		if i == num - 1 {
			pears[i].Next = pears[0]
		}
	}
	circle := &Circle{
		Pears:   pears,
		CurrentIdx:  0,
		CurrentPear: pears[0],
	}
	return circle
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		cnt, err := rd.ReadString('\n')
		if err != nil {
			log.Fatal("Read conn content error ,err cnt: ", err)
		}
		if err == io.EOF {
			break
		}
		circle.AddTask(nil)
		fmt.Printf("%s \n", cnt)
		fmt.Println("read connection cnt finished! \n")
	}
}

func unpack(cnt []byte) (msg Msg) {
	err := msgpack.Unmarshal(cnt, &msg)
	if err != nil {
		log.Fatal("An error occurred while unpacking a msgpack package, error cnt :", err)
	}
	return
}

var circle *Circle

func main(){
	ls, err := net.Listen("tcp", "0.0.0.0:8787")
	if err != nil {
		log.Fatal(err)
	}
	circle = NewCircle(6)
	go circle.Run()
	for {
		conn, err := ls.Accept()
		if err != nil {
			log.Fatal("Get client connection error, err cnt : ", err)
		}
		go handleConn(conn)
	}
}
