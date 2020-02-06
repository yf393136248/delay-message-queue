package main

import (
	"bufio"
	"bytes"
	"delay-message-queue/mq"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os/exec"
	"strconv"
)

var (
	circleMq *mq.Node
	circleSlotNum int
	tcpPort int
)

//队列添加的请求消息体
type Msg struct {
	Type string
	Script string
	Params []interface{}
	Interval int32
}

func main(){
	flag.IntVar(&circleSlotNum, "slot", 60, "需要创建卡槽的数量，1秒一个")
	flag.IntVar(&tcpPort, "port", 8080, "tcp监听端口")
	flag.Parse()

	circleMq = mq.NewCirCleMq(circleSlotNum)
	circleMq.Run(8080, connResolve)
}

func connResolve(conn *net.TCPConn) {
	defer conn.Close()
	fmt.Println(conn.RemoteAddr().String())
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			fmt.Println(err)
			conn.Write([]byte("{\"code\": 400, \"msg\": \"fail\"}\n"))
			continue
		}
		body := Msg{}
		if err := json.Unmarshal(msg, &body); err != nil {
			fmt.Println(err)
			conn.Write([]byte("{\"code\": 400, \"msg\": \"fail\"}\n"))
			continue
		}
		if body.Type == "" {
			conn.Write([]byte("{\"code\": 400, \"msg\": \"fail\"}\n"))
			continue
		}
		circleNum := int(body.Interval) / circleSlotNum
		job := &mq.Job{
			Circles: circleNum,
			PlusNodeNum: int(body.Interval) - circleNum * circleSlotNum,
			Type:    body.Type,
			Script:  body.Script,
			Params:  body.Params,
			Callback: jobCallbak,
		}
		circleMq.PushJob(job)
		conn.Write([]byte("{\"code\": 200, \"msg\": \"ok\"}\n"))
	}

}

func jobCallbak(job *mq.Job) {
	params := make([]string, 0)
	if len(job.Params) > 0 {
		for _, v := range job.Params {
			if _v, ok := v.(string); ok {
				params = append(params, _v)
			}
			if _v, ok := v.(int); ok {
				params = append(params, strconv.Itoa(_v))
			}
		}
	}
	out := bytes.Buffer{}
	cmd := exec.Command(job.Script, params...)
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		fmt.Printf("脚本运行错误，错误原因：%s", err)
		return
	}
	fmt.Printf("脚本运行完成，输出内容：%s", out.String())
}

