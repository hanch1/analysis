package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	START_STR = "127.0.0.1--"
	END_STR = "/HTTP"
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

// 日志解析模块：消费chanel中日志
func logConsumer(logChannel chan string, pvChannel, uvChannel chan urlData) error{
	// 从logChannel中消费日志
	for logStr := range logChannel {
		// 切割日志字符串，拿出需要的数据
		need := cutLogFetchData(logStr)

		// uid ：模拟一个用户id
		hasher := md5.New()
		hasher.Write([]byte(need.refer + need.ua))
		uid := hex.EncodeToString(hasher.Sum(nil))

		// 使用解析到的数据构建 urlData
		uData := urlData{need,uid}

		log.Infoln(uData)

		// 放入channel中用于统计
		pvChannel <- uData
		uvChannel <- uData
	}
	return nil
}

// 切割日志字符串，拿出需要的数据
func cutLogFetchData(logStr string) digData {
	// 去除空格
	logStr = strings.TrimSpace(logStr)
	startIndex := str.IndexOf(logStr, START_STR, 0)
	if startIndex == -1{
		return digData{}
	}
	startIndex += len(START_STR)
	endIndex := str.IndexOf(logStr, END_STR, startIndex)

	d := str.Substr(logStr, startIndex, endIndex - startIndex)

	urlInfo, err := url.Parse("http://127.0.0.1/?" + d)
	if err != nil {
		return digData{}
	}
	data := urlInfo.Query()
	// 返回需要的信息
	needData := digData{
		data.Get("time"),
		data.Get("refer"),
		data.Get("url"),
		data.Get("ua"),
	}

	return needData
}

// 统计分析模块：逐行消费日志
func readFileByLine(params cmdParams, logChannel chan string) {
	file, err := os.Open(params.logPath)
	if err != nil {
		log.Warningf("[readFileByLine] can't open file: %v", err)
	}
	defer file.Close()

	count := 0
	bufReader := bufio.NewReader(file)
	for  {
		line, err := bufReader.ReadString('\n')
		logChannel <- line
		count++
		if count % (2 * params.routineNum) == 0{
			log.Infof("[readFileByLine] read %d line", count)
		}
		if err != nil {
			if err == io.EOF{
				time.Sleep(3 * time.Second)
				log.Infof("[readFileByLine] wait, read %d line", count)
			}else {
				log.Warningf("[readFileByLine] read log error: %s", err)
			}
		}
	}
}
