package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/vmihailenco/msgpack"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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
	Slots	[]*Slot	//slot组
	Next	*Pear
	Index   int
}

type Slot struct {
	Script 	string	//脚本名称
	Params  string	//脚本参数
	CircleNum	int	//循环几圈后执行本slot内部的脚本
	SerializeIndex   int     //序列化使用，表示为放在第几个Pear的下面，其他时候无效
}

//响应给客户端内容
type Resp struct {
	Flag  int	`json:"flag"`
	Msg   string`json:"msg"`
}

//序列化存储的结构体
type Serialize struct {
	Slots  []Slot
	CurrentIndex 	int
}

func (c *Circle) serialize() ([]byte, error){
	slots := make([]Slot, 0)
	for idx, pear := range c.Pears {
		for _, slot := range pear.Slots {
			slot.SerializeIndex = idx
			slots = append(slots, *slot)
		}
	}
	return json.Marshal(Serialize{Slots: slots, CurrentIndex:c.CurrentIdx})
}

func (c *Circle) Save() bool{
	return true
}

func (c *Circle) Run() {
	T := time.NewTicker(time.Second)
	for {
		select {
		case <-T.C:
			c.CurrentPear = c.CurrentPear.Next
			c.CurrentIdx  = c.CurrentPear.Index
			fmt.Println("运行...", c.CurrentIdx)
			c.CurrentPear.checkSlots()
		}
	}
}

func (c *Circle) AddTask(slot *Slot, newIdx int) {
	fmt.Println("接收到新任务", c.CurrentIdx)
	targetPear := c.Pears[newIdx]
	targetPear.Slots = append(targetPear.Slots, slot)
}

func (c *Circle) genSlotIdx(after int) (idx, num int) {
	cidx := c.CurrentIdx + after
	circle_len := len(c.Pears)
	circle_num := math.Floor(float64(after) / float64(circle_len))
	cidx += after % circle_len
	//超出部分将回滚到起点重新计算
	if cidx >= circle_len {
		cidx -= circle_len
	}
	//如果刚好是长度的倍数，那么循环次数需减去一次
	if after % circle_len == 0 {
		circle_num -= 1
	}
	return cidx, int(circle_num)
}

func NewCircle(num int) *Circle {
	if num < 2 {
		log.Fatal("环形队列的元素块数量不能小于2！")
	}
	pears := make([]*Pear, num)
	for i := 0; i < num; i++ {
		p := &Pear{
			Slots:  make([]*Slot, 0),
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

func (p *Pear) checkSlots() {
	for k, slot := range p.Slots {
		if slot.CircleNum > 0 {
			slot.CircleNum--
		}else{
			//执行脚本，并移除该slot
			p.Slots = append(p.Slots[:k], p.Slots[k+1:]...)
			slot.run()
		}
	}
}

func (s *Slot) run() {
	cmd := exec.Command(s.Script, s.Params)
	if err := cmd.Run(); err != nil {
		log.Println("Run script failed, failed reason: ", err, "script name: ", s.Script, "params: ", s.Params)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		cnt, err := rd.ReadBytes('\n')
		if err != nil {
			log.Fatal("Read conn content error ,err cnt: ", err)
		}
		if err == io.EOF {
			break
		}
		//对接收到的二进制内容进行解包
		msg  := unpack(cnt)
		cidx, circle_num := circle.genSlotIdx(msg.After)
		slot := Slot{
			Script: msg.Script,
			Params: msg.Params,
			CircleNum: circle_num,
		}
		resp := pack(Resp{0, ""})
		conn.Write(resp)
		circle.AddTask(&slot, cidx)
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

func pack(resp Resp) []byte {
	cnt, _ := msgpack.Marshal(resp)
	return cnt
}

//守护进程运行
func daemonMode() {
	if os.Getpid() != 1 {
		filePath, _ := filepath.Abs(os.Args[0])
		args := append([]string{filePath}, os.Args[1:]... )
		os.StartProcess(filePath, args, &os.ProcAttr{Files: []*os.File {os.Stdin, os.Stdout, os.Stderr}})
		return
	}
}

var circle *Circle
var savePath *string

func main(){
	port := flag.String("port", "8787", "port which you want specified...")
	daemon := flag.Bool("D", false, "run at daemon mode...")
	savePath = flag.String("file", filepath.Dir(os.Args[0]), "save path")
	flag.Parse()
	fmt.Println(*savePath)
	if *daemon {
		daemonMode()
	}
	ls, err := net.Listen("tcp", "0.0.0.0:"+ *port)
	if err != nil {
		log.Fatal(err)
	}
	//对环形队列的初始化操作
	circle = NewCircle(6)
	go circle.Run()
	//对指定端口进行监听
	for {
		conn, err := ls.Accept()
		if err != nil {
			log.Fatal("Get client connection error, err cnt : ", err)
		}
		go handleConn(conn)
	}
}

