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
		keyType    RedisType
		err        error
	}

	RedisType string
)

const (
	RedisHash   RedisType = "hash"
	RedisString RedisType = "string"
	RedisNone   RedisType = "none"
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

	a.keyType = RedisType(val.(string))
	if a.keyType == RedisNone {
		a.err = fmt.Errorf("GetRedisKeyType: key %s does not exist", a.key)
	}
	return a
}

// CopyHMData - copy data from redis
func (a *AppCtx) CopyHMData() *AppCtx {
	if a.err != nil {
		return a
	}
	if a.keyType != RedisHash {
		a.err = fmt.Errorf("CopyHMData: invalid data type %s", a.keyType)
	}
	// query source db
	iter := a.srcClient.HScan(a.ctx, a.key, 0, "", 1000).Iterator()
	if err := iter.Err(); err != nil {
		a.err = fmt.Errorf("CopyHMData: %v", err)
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
		a.err = fmt.Errorf("CopyHMData: %v", err)
		return a
	}
	if !res {
		a.err = fmt.Errorf("CopyHMData: could not save to dest, response: %v", res)
		return a
	}
	return a
}

// CopyStringData - copy data from redis
func (a *AppCtx) CopyStringData() *AppCtx {
	if a.err != nil {
		return a
	}
	if a.keyType != RedisString {
		a.err = fmt.Errorf("CopyStringData: invalid data type %s", a.keyType)
	}
	// query source db
	srcVal, err := a.srcClient.Get(a.ctx, a.key).Result()
	if err != nil {
		a.err = fmt.Errorf("CopyStringData: error while fetching data: %v", err)
		return a
	}

	// insert into dest
	res, err := a.destClient.Set(a.ctx, a.key, srcVal, -1).Result()
	if err != nil {
		a.err = fmt.Errorf("CopyRedisData: %v", err)
		return a
	}
	if res != "OK" {
		a.err = fmt.Errorf("CopyStringData: could not save to dest, response: %s", res)
		return a
	}
	return a
}

// Copy - copy data from source to dest
func (a *AppCtx) Copy() (err error) {
	if a.err != nil {
		return a.Error()
	}
	if a.keyType == RedisHash {
		a = a.CopyHMData()
	} else if a.keyType == RedisString {
		a = a.CopyStringData()
	} else {
		return fmt.Errorf("Unsupported redis key type: %s", string(a.keyType))
	}
	err = a.Error()
	if err != nil {
		return
	}
	return nil
}

// Error - get error
func (a *AppCtx) Error() error {
	return a.err
}

func main() {
	appCtx := ParseCliArgs().GetAppContext().GetRedisKeyType()
	err := appCtx.Copy()
	if err != nil {
		fmt.Println("ERROR:", err)
	} else {
		fmt.Println("Done")
	}
}
