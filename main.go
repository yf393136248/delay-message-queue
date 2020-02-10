package main

import (
	"delay-message-queue/mq"
	"flag"
	"fmt"
)

const (
	LOG_FILE_PATH = "log/mq.log"
)

var (
	circleMq *mq.Node
	circleSlotNum int
	tcpPort int
)


func main(){
	flag.IntVar(&circleSlotNum, "slot", 60, "需要创建卡槽的数量，1秒一个")
	flag.IntVar(&tcpPort, "port", 8080, "tcp监听端口")
	flag.Parse()

	circleMq, err := mq.NewCirCleMq(circleSlotNum, LOG_FILE_PATH)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("is running, binding port：%d, slot num:%d\n", tcpPort, circleSlotNum)
	circleMq.Run(8080)
}
