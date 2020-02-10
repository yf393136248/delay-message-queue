package util

import (
	"log"
	"os"
)


type Log struct {
	FilePath string
	FileHandler *os.File
	*log.Logger
}

func NewLogs(filePath string) (*Log, error) {
	fp, err := os.OpenFile(filePath, os.O_APPEND | os.O_RDWR | os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	logger := log.New(fp, "", 0)
	return &Log{filePath, fp, logger}, nil
}

func (l *Log) Debug(cnt string) {
	l.Logger.Printf("[debug] err cnt [%s]\n", cnt)
}

func (l *Log) Warning(cnt string) {
	l.Logger.Printf("[warning] err cnt [%s]\n", cnt)
}

func (l *Log) Fatal(cnt string) {
	l.Logger.Fatalf("[fatal] err cnt [%s]\n", cnt)
}

func (l *Log) Panic(cnt string) {
	l.Logger.Panicf("[panic] err cnt [%s]\n", cnt)
}