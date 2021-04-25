package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
		DB:       0,
	})
	defer client.Close()
	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal("redis connect failed")
	}

	// test
	client.Set("test01", 1, -1)
	client.Set("test02", 1, -1)
	// pfadd
	res := client.PFAdd("pftest", 1)
	client.PFAdd("pftest", 2)
	client.PFAdd("pftest", 3)
	fmt.Printf("args: %v\n", res.Args())
	fmt.Printf("cmd name: %v\n", res.Name())
	fmt.Printf("return val: %v\n", res.Val())
	fmt.Println(client.PFCount("pftest"))
}
