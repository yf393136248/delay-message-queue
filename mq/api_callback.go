package mq

import (
	"delay-message-queue/util"
	"fmt"
	"net/http"
)

func jobApiCallback(job *Job, log *util.Log) {
	resp, err := http.Get(job.Script)
	if err != nil {
		log.Debug(err.Error())
		return
	}
	defer resp.Body.Close()
	body := make([]byte, 0)
	if _, err := resp.Body.Read(body); err != nil {
		log.Debug("http response body read error " + err.Error())
		return
	}
	log.Debug(fmt.Sprintf("job api call resp: [%d] [%s]", resp.StatusCode, string(body)))
}
