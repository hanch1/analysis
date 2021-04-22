package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type resource struct {
	url    string
	target string
	start  int
	end    int
}

func ruleResource() []resource {
	var res []resource
	// 首页
	r1 := resource{
		url:    "http://localhost:8888/",
		target: "",
		start:  0,
		end:    0,
	}
	// 列表页
	r2 := resource{
		url:    "http://localhost:8888/list/{$id}.html",
		target: "{$id}",
		start:  1,
		end:    21,
	}
	// 详情页
	r3 := resource{
		url:    "http://localhost:8888/movie/{$id}.html",
		target: "{$id}",
		start:  1,
		end:    12924,
	}
	res = append(res, r1, r2, r3)
	return res
}

// 批量生成url
func buildUrl(res []resource) []string {
	var list []string
	for _, item := range res {
		if len(item.target) == 0 {
			list = append(list, item.url)
		} else {
			for i := item.start; i <= item.end; i++ {
				// 替换url
				urlStr := strings.Replace(item.url, item.target, strconv.Itoa(i), -1)
				list = append(list, urlStr)
			}
		}
	}
	return list
}
func randInt(min, max int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano())) // 随机种子
	if min >= max {
		return max
	}
	return r.Intn(max-min) + min
}

// 生成日志
func makeLog(current, refer, ua string) string {
	u := url.Values{}
	u.Set("time", "1")
	u.Set("url", current)
	u.Set("refer", refer)
	u.Set("ua", ua)

	paramStr := u.Encode()
	logTemplate := "127.0.0.1--{$params}--{$ua}"
	log := strings.Replace(logTemplate, "{$params}", paramStr, -1)
	log = strings.Replace(log, "{$ua", ua, -1)
	return log
}

// 生成日志函数
func main() {
	// 获取命令行参数
	total := flag.Int("total", 100, "how many rows by created")
	filePath := flag.String("filePath", "F:\\go_project\\src\\analysis\\logs\\dig.log", "file path")
	flag.Parse()

	// 构造网站url集合
	resources := ruleResource()
	url := buildUrl(resources)

	// 根绝url生成total行日志内容
	var logStr string
	for i := 0; i < *total; i++ {
		// 随机选择
		currentUrl := url[randInt(0, len(url)-1)]
		referUrl := url[randInt(0, len(url)-1)]
		ua := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
			"(KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36"
		logStr += makeLog(currentUrl, referUrl, ua) + "\n"
	}
	// 追加写
	file, err := os.OpenFile(*filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("open file error: %v\n", err)
	}
	_, err = file.Write([]byte(logStr))
	if err != nil {
		fmt.Printf("write file error: %v\n", err)
	}
	file.Close()

	fmt.Println("done")
}
