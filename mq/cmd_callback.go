package mq

import (
	"bytes"
	"delay-message-queue/util"
	"os/exec"
	"strconv"
)

func jobCmdCallback(job *Job, log *util.Log) {
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
		log.Debug("脚本运行错误，错误原因：" + err.Error())
		return
	}
	log.Debug("脚本运行完成，输出内容："+ out.String())
}
