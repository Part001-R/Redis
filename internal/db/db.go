package db

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

var once sync.Once

// Object.
type objectRedis struct {
	db *redis.Client
}

// Interface.
type RedisI interface {
	Close() error
	Ping() error
	SendStringTTL(k, v string, ttl time.Duration) error
	RecvString(k string) (string, error)
	CheckExistsKey(k string) (result int64, err error)
}

var inst *objectRedis

// Constructor.
func New(addr, pwd string, dbNum int) (inf RedisI, err error) {

	// Check.
	if addr == "" {
		return nil, ErrNilDBPointer
	}
	if dbNum < 0 {
		return nil, fmt.Errorf("number of db less 0:<%d>", dbNum)
	}

	// Logic.
	once.Do(func() {
		rdb := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: pwd,
			DB:       dbNum,
		})

		inst = &objectRedis{
			db: rdb,
		}
	})

	return inst, nil
}

// Close DB connect. Return error.
func (r *objectRedis) Close() error {

	// Check.
	if r.db == nil {
		return ErrNilDBPointer
	}

	// Logic.
	err := r.db.Close()
	if err != nil {
		return fmt.Errorf("Fault close connect: <%w>", err)
	}

	once = sync.Once{}
	inst = nil

	return nil
}

// Check connect. Return error.
func (r *objectRedis) Ping() error {

	// Check.
	if r.db == nil {
		return ErrNilDBPointer
	}

	// Logic.
	pong, err := r.db.Ping().Result()
	if err != nil {
		return fmt.Errorf("fault ping Redis: %v", err)
	}
	if pong != "PONG" {
		return fmt.Errorf("responce not PONG:{%s}", pong)
	}
	return nil
}

// Send string with TTL. Return error.
//
// Params:
//
//	r - key.
//	v - value.
//	ttl - TTL.
func (r *objectRedis) SendStringTTL(k, v string, ttl time.Duration) error {

	// Check.
	if r.db == nil {
		return ErrNilDBPointer
	}
	if k == "" {
		return ErrMissingKey
	}
	if v == "" {
		return ErrMissingValue
	}
	if ttl <= 0 {
		return ErrMissingTTL
	}

	// Logic.
	err := r.db.Set(k, v, ttl).Err()
	if err != nil {
		return fmt.Errorf("fault Tx data: %v", err)
	}
	return nil
}

// Recieve string by key. Return string and error.
//
// Params:
//
//	k - key.
func (r *objectRedis) RecvString(k string) (string, error) {

	// Check.
	if r.db == nil {
		return "", ErrNilDBPointer
	}

	// Logic.
	retrievedValue, err := r.db.Get(k).Result()
	if err != nil {
		return "", fmt.Errorf("fault Get value by key{%s}: %v", k, err)
	}
	return retrievedValue, nil
}

// Check exists key. Return 1 - if key exists and error.
//
// Params:
//
//	k - key.
func (r *objectRedis) CheckExistsKey(k string) (result int64, err error) {

	// Check.
	if r.db == nil {
		return 0, ErrNilDBPointer
	}
	if k == "" {
		return 0, ErrMissingKey
	}

	// Logic.
	result, err = r.db.Exists(k).Result()
	if err != nil {
		return 0, fmt.Errorf("Function Exists, returned error: <%w>", err)
	}

	return result, nil
}
