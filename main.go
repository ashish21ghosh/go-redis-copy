package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/go-redis/redis/v8"
)

type (
	CLIArgs struct {
		Src  string `json:"src"`
		Dest string `json:"dest"`
		Key  string `json:"key"`
	}

	AppCtx struct {
		srcClient  *redis.Client
		destClient *redis.Client
		ctx        context.Context
		key        string
		keyType    string
		err        error
	}
)

// ParseCliArgs - parses command line arguments
func ParseCliArgs() (cliArgs *CLIArgs) {
	src := flag.String("src", "localhost:6379", "redis source connections URL")
	dest := flag.String("dest", "localhost:6379", "redis destination connections URL")
	key := flag.String("key", "", "redis key to be replicated")

	flag.Parse()
	cliArgs = &CLIArgs{
		Src:  *src,
		Dest: *dest,
		Key:  *key,
	}

	js, _ := json.MarshalIndent(cliArgs, "", "  ")
	log.Printf("command_line_args: Config (%s)\n", js)
	return cliArgs
}

// GetRedisClient - Get redis client
func GetRedisClient(url string) *redis.Client {
	opt, err := redis.ParseURL(fmt.Sprintf("redis://%s/0", url))
	if err != nil {
		panic(err)
	}

	rdb := redis.NewClient(opt)
	return rdb
}

// GetAppContext - get app context
func (c *CLIArgs) GetAppContext() *AppCtx {
	// Get source client
	srcClient := GetRedisClient(c.Src)
	destClient := GetRedisClient(c.Dest)

	return &AppCtx{
		srcClient:  srcClient,
		destClient: destClient,
		key:        c.Key,
		ctx:        context.Background(),
	}
}

// GetRedisKeyType - get redis key type
func (a *AppCtx) GetRedisKeyType() *AppCtx {
	if a.err != nil {
		return a
	}
	// query key type
	val, err := a.srcClient.Do(a.ctx, "type", a.key).Result()
	if err != nil {
		a.err = err
		return a
	}

	a.keyType = val.(string)
	if a.keyType == "none" {
		a.err = fmt.Errorf("GetRedisKeyType: key %s does not exist", a.key)
	}
	return a
}

// CopyHMData - copy data from redis
func (a *AppCtx) CopyHMData() *AppCtx {
	if a.err != nil {
		return a
	}
	if a.keyType != "hash" {
		a.err = fmt.Errorf("CopyHMData: invalid data type %s", a.keyType)
	}
	// query source db
	iter := a.srcClient.HScan(a.ctx, a.key, 0, "", 1000).Iterator()
	if err := iter.Err(); err != nil {
		a.err = fmt.Errorf("CopyRedisData: %v", err)
		return a
	}

	var args = []interface{}{}
	for iter.Next(a.ctx) {
		val := iter.Val()
		args = append(args, val)
	}

	// insert into dest
	res, err := a.destClient.HMSet(a.ctx, a.key, args...).Result()
	if err != nil {
		a.err = fmt.Errorf("CopyRedisData: %v", err)
		return a
	}
	if !res {
		a.err = fmt.Errorf("CopyRedisData: %v", err)
		return a
	}
	return a
}

// Error - get error
func (a *AppCtx) Error() error {
	return a.err
}

func main() {
	val := ParseCliArgs().GetAppContext().GetRedisKeyType().CopyHMData()
	err := val.Error()
	if err != nil {
		fmt.Println("ERROR:", err)
	}
}
