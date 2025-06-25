package db

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

type ObjectRedis struct {
	Redis *redis.Client
}

type iRedis interface {
	PingRedis() error
	SetData(k, v string, ttl time.Duration) error
	GetData(k string) (string, error)
}

func InstanceRedis(addr, pwd string, dbNum int) (*redis.Client, func(), error) {
	if addr == "" {
		return nil, nil, errors.New("missed addres db")
	}
	if dbNum < 0 {
		return nil, nil, fmt.Errorf("nmber db less 0:{%d}", dbNum)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       dbNum,
	})

	fc := func() {
		err := rdb.Close()
		if err != nil {
			fmt.Printf("fault close db connect:{%v}\n", err)
		}
	}

	return rdb, fc, nil
}

func InterfaceRedis(r *redis.Client) (iRedis, error) {
	if r == nil {
		return nil, errors.New("missed pointer Redis")
	}
	return &ObjectRedis{
		Redis: r,
	}, nil
}

func (r *ObjectRedis) PingRedis() error {
	pong, err := r.Redis.Ping().Result()
	if err != nil {
		return fmt.Errorf("fault ping Redis: %v", err)
	}
	if pong != "PONG" {
		return fmt.Errorf("responce not PONG:{%s}", pong)
	}
	return nil
}

func (r *ObjectRedis) SetData(k, v string, ttl time.Duration) error {
	err := r.Redis.Set(k, v, ttl).Err()
	if err != nil {
		return fmt.Errorf("fault Tx data: %v", err)
	}
	return nil
}

func (r *ObjectRedis) GetData(k string) (string, error) {
	retrievedValue, err := r.Redis.Get(k).Result()
	if err != nil {
		return "", fmt.Errorf("fault Get value by key{%s}: %v", k, err)
	}
	return retrievedValue, nil
}
