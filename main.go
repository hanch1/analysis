package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"github.com/go-redis/redis"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	START_STR = "127.0.0.1--"
	END_STR = "/HTTP"
	HANDLE_MOVIE = "/movie/"
    HANDLE_LIST = "/list/"
    HANDLE_HTML = ".html"
)

type cmdParams struct {
	logPath    string
	routineNum int
	l          string
}

// channel中进行信息传输的
type urlData struct {
	data digData  // 挖掘出的数据
	uid  string
	un   urlNode
}

type digData struct {
	time  string
	url   string
	refer string
	ua    string
}

type storageBlock struct {
	counterType  string
	// 采用什么数据库存储 redis
	storageModel string
	un        urlNode
}

// 存储用的
type urlNode struct {
	unType string   // 详情页/列表页/首页
	unRid int       // Resource ID 资源id
	unUrl string    // 当前页面url
	unTime string   // 访问当前页面的时间
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

	// redis连接池
	redisPool := redis.NewClient(&redis.Options{
		//连接信息
		Network:  "tcp",                  //网络类型，tcp or unix，默认tcp
		Addr:     "127.0.0.1:6379",       //主机名+冒号+端口，默认localhost:6379
		Password: "123456",               //密码
		DB:       0,                      // redis数据库index

		//连接池容量及闲置连接数量
		PoolSize:     2 * params.routineNum, // 连接池最大socket连接数，默认为4倍CPU数， 4 * runtime.NumCPU
		MinIdleConns: params.routineNum,     //在启动阶段创建指定数量的Idle连接，并长期维持idle状态的连接数不少于指定数量；。

		//超时
		DialTimeout:  5 * time.Second, //连接建立超时时间，默认5秒。
		ReadTimeout:  3 * time.Second, //读超时，默认3秒， -1表示取消读超时
		WriteTimeout: 3 * time.Second, //写超时，默认等于读超时
		PoolTimeout:  4 * time.Second, //当所有连接都处在繁忙状态时，客户端等待可用连接的最大等待时长，默认为读超时+1秒。

		//闲置连接检查包括IdleTimeout，MaxConnAge
		IdleCheckFrequency: 60 * time.Second, //闲置连接检查的周期，默认为1分钟，-1表示不做周期性检查，只在客户端获取连接时对闲置连接进行处理。
		IdleTimeout:        5 * time.Minute,  //闲置超时，默认5分钟，-1表示取消闲置超时检查
		MaxConnAge:         0 * time.Second,  //连接存活时长，从创建开始计时，超过指定时长则关闭连接，默认为0，即不关闭存活时长较长的连接

		//命令执行失败时的重试策略
		MaxRetries:      0,                      // 命令执行失败时，最多重试多少次，默认为0即不重试
		MinRetryBackoff: 8 * time.Millisecond,   //每次计算重试间隔时间的下限，默认8毫秒，-1表示取消间隔
		MaxRetryBackoff: 512 * time.Millisecond, //每次计算重试间隔时间的上限，默认512毫秒，-1表示取消间隔

	})
	_, err = redisPool.Ping().Result()
	if err != nil {
		log.Fatalln("redis pool created failed.")
		panic(err)
	} else {
		go func() {

		}()
		defer redisPool.Close()
	}


	// 日志消费者
	go readFileByLine(params, logChannel)

	// 创建一组日志处理者
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}

	// 创建 PV UV 统计器  (可拓展 xxxConunter)
	go pvCounter(pvChannel, storeChannel)
	go uvCounter(uvChannel, storeChannel, redisPool)

	// 创建 存储器
	go dataStorage(storeChannel, redisPool)

	time.Sleep(1 * time.Hour)
}

func dataStorage(storeChannel chan storageBlock, redisPool *redis.Client) {

}

// 统计分析模块 PV(页面浏览量或点击量) UV(访问人数)
// PV Channel
func pvCounter(pvChannel chan urlData, storeChannel chan storageBlock) {
	// 消费数据
	for data := range pvChannel {
		storeData := storageBlock{"pv", "ZINCREBY", data.un}
		storeChannel <- storeData
	}
}
// UV Channel (需要去重)
func uvCounter(uvChannel chan urlData, storeChannel chan storageBlock, redisPool *redis.Client) {
	for udata := range uvChannel {
		// 去重  HyperLogLog  redis
		hyperLogLogkey := "uv_hpll_" + getTime(udata.data.time, "day")
		res := redisPool.PFAdd(hyperLogLogkey, udata.uid)
		//  一天内用户不能重复
		redisPool.Expire(hyperLogLogkey, 24 * time.Hour)
		if res.Val() == 1{
			// 说明 hyperloglog中不存在
			continue
		}

		sItem := storageBlock{
			counterType:  "uv",
			storageModel: "ZINCRBY",
			un:           udata.un,
		}
		storeChannel <- sItem
	}
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
		uData := urlData{need,uid, formatUrl(need.url, need.time)}

		log.Infoln(uData)

		// 放入channel中用于统计
		pvChannel <- uData
		uvChannel <- uData
	}
	return nil
}

func formatUrl(url, time string) urlNode {
	// 一定从量大的着手,  详情页>列表页≥首页
	pos1 := str.IndexOf(url, HANDLE_MOVIE, 0)
	// 从url中获取id
	if pos1 != -1 {
		pos1 += len(HANDLE_MOVIE)
		pos2 := str.IndexOf(url, HANDLE_HTML, 0)
		idStr := str.Substr(url, pos1, pos2 - pos1)
		id, _ := strconv.Atoi(idStr)
		return urlNode{"movie", id, url, time}
	} else {
		pos1 = str.IndexOf(url, HANDLE_LIST, 0)
		if pos1 != -1 {
			pos1 += len(HANDLE_LIST)
			pos2 := str.IndexOf(url, HANDLE_HTML, 0)
			idStr := str.Substr(url, pos1, pos2-pos1)
			id, _ := strconv.Atoi(idStr)
			return urlNode{"list", id, url, time}
		} else {
			return urlNode{"home", 1, url, time}
		} // 如果页面url有很多种，就不断在这里扩展
	}
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

func getTime(logTime, timeType string) string{
	// 时间格式
	var format string
	switch timeType {
	case "day":
		format = "2020-01-01"
		break
	case "hour":
		format = "2020-01-01 01"
		break
	case "min":
		format = "2020-01-01 01:01"
		break
	}
	t, _ := time.Parse(format, time.Now().Format(format))

	return strconv.FormatInt(t.Unix(), 10)
}