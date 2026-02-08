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
	// Close connect.
	Close() error
	// Check connect.
	Ping() error
	// Send string.
	SendStringTTL(k, v string, ttl time.Duration) error
	// Request string.
	RecvString(k string) (string, error)
	// Check exists key.
	CheckExistsKey(k string) (result int64, err error)
	// Update string value.
	UpdateStringTTL(k, v string, ttl time.Duration) error
	// Update string value and recieve previous value.
	UpdateStringRecievePrev(k, v string) (string, error)
	// Send multiple strings.
	SendMultipleStrings(data map[string]string) (result string, err error)
	// Recieve multiple strings.
	RecvMultipleStrings(keys []string) (values []string, err error)
	// Delete string.
	Delete(key string) error
	// Delete multyple strings.
	DeleteMultipleStrings(keys []string) error
	// Send new integer value.
	SendIntegerTTL(k string, v int64, ttl time.Duration) (err error)
	// Increment exists integer value.
	IncrementInteger(k string) (result int64, err error)
	// Adding integer value.
	AddInteger(k string, v int64) (result int64, err error)
	// Decrement exists key integer.
	DecrementInteger(k string) (result int64, err error)
	// Subtraction exists key integer.
	SubInteger(k string, v int64) (result int64, err error)
	// Send new key with ttl.
	SendFloatTTL(k string, v float64, ttl time.Duration) (err error)
	// Adding float value.
	AddFloat(k string, v float64) (result float64, err error)
	// Send new JSON.
	SendNewJSONTTL(key string, value TestJSON, ttl time.Duration) error
	// Recieve JSON.
	RecvJSON(key string) (TestJSON, error)
	// Update JSON.
	UpdateJSONTTL(key string, value TestJSON, ttl time.Duration) error
	// Send new list of strings.
	ListStringNew(key string, values []string) (err error)
	// Add to the left side exists list.
	ListStringAddLeft(key string, values []string) (err error)
	// Add to the right side exists list.
	ListStringAddRight(key string, values []string) (err error)
	// Recieve value from left side list.
	ListStringRecvLeft(key string) (value string, err error)
	// Recieve value from right side list.
	ListStringRecvRight(key string) (value string, err error)
	// Recieve len of exists list.
	ListStringRecvLen(key string) (value int64, err error)
	// Move element of list dy name to the new list.
	ListStringMoveByNameToNewLeft(srcKey, destKey string, value string) (int64, error)
	// Recieve range of values of exists list.
	ListStringRecvRange(key string, indStart, indStop int64) (values []string, err error)
	// Trim range values of exists list.
	ListStringTrimRange(key string, indStart, indStop int64) (result string, err error)
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
