package main

import (
	"flag"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

type cmdParams struct {
	logPath    string
	routineNum int
	l          string
}

// channel中进行信息传输的
type urlData struct {
	data digData
	uid  string
}

type digData struct {
	time  string
	url   string
	refer string
	ua    string
}

type storageBlock struct {
	counterType  string
	storageModel string
	unode        urlNode
}

// 存储用的
type urlNode struct {
}

// 日志
var log = logrus.New()

func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
}

func main() {
	// 获取参数
	logPath := flag.String("logPath", "F:\\go_project\\src\\analysis\\logs\\dig.log", "log path")
	routineNum := flag.Int("routineNum", 5, "goroutine num")
	// 程序运行时的日志位置
	l := flag.String("l", "F:\\go_project\\src\\analysis\\logs\\tmp.log", "tmp log")
	flag.Parse()

	params := cmdParams{
		logPath:    *logPath,
		routineNum: *routineNum,
		l:          *l,
	}
	// 打日志
	logFd, err := os.OpenFile(params.l, os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		log.Out = logFd
		defer logFd.Close()
	}
	log.Info("程序启动")
	log.Infof("Params: logPath=%s, routineNum=%d", params.logPath, params.routineNum)

	// 初始化channel，用于数据传递
	var logChannel = make(chan string, 3*params.routineNum)
	var pvChannel = make(chan urlData, params.routineNum)
	var uvChannel = make(chan urlData, params.routineNum)
	var storeChannel = make(chan storageBlock, params.routineNum)

	// 日志消费者
	go readFileByLine(params, logChannel)
	// 创建一组日志处理者
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}
	// 创建 PV UV 统计器  (可拓展 xxxConunter)
	go pvCounter(pvChannel, storeChannel)
	go uvCounter(pvChannel, storeChannel)
	// 创建 存储器
	go dataStorage(storeChannel)

	time.Sleep(1 * time.Hour)
}

func dataStorage(storeChannel chan storageBlock) {

}

func pvCounter(pvChannel chan urlData, storeChannel chan storageBlock) {

}

func uvCounter(uvChannel chan urlData, storeChannel chan storageBlock) {

}

func logConsumer(logChannel chan string, pvChannel, uvChannel chan urlData) {

}

func readFileByLine(params cmdParams, logChannel chan string) {

}
