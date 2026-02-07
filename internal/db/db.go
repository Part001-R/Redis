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
	DeleteString(key string) error
	// Delete multyple strings.
	DeleteMultipleStrings(keys []string) error
	// Send new integer value.
	SendIntegerTTL(k string, v int64, ttl time.Duration) (err error)
	// Increment exists integer value.
	IncrementInteger(k string) (result int64, err error)
	// Adding integer value.
	AddInteger(k string, v int64) (result int64, err error)
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

// Send new string with TTL. Return error.
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
	err := r.db.SetNX(k, v, ttl).Err()
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

// Update exists string with TTL. Return error.
//
// Params:
//
//	r - key.
//	v - value.
//	ttl - TTL.
func (r *objectRedis) UpdateStringTTL(k, v string, ttl time.Duration) error {

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
	err := r.db.SetXX(k, v, ttl).Err()
	if err != nil {
		return fmt.Errorf("fault update data: %v", err)
	}
	return nil
}

// Update string with TTL. Return previous value and error.
//
// Params:
//
//	r - key.
//	v - value.
func (r *objectRedis) UpdateStringRecievePrev(k, v string) (string, error) {

	// Check.
	if r.db == nil {
		return "", ErrNilDBPointer
	}
	if k == "" {
		return "", ErrMissingKey
	}
	if v == "" {
		return "", ErrMissingValue
	}

	// Logic.
	value, err := r.db.GetSet(k, v).Result()
	if err != nil {
		return "", fmt.Errorf("fault update data: %v", err)
	}

	return value, nil
}

// Send multiple values. Return error.
//
// Params:
//
//	data - map of key/value.
func (r *objectRedis) SendMultipleStrings(data map[string]string) (result string, err error) {

	// Check
	if r.db == nil {
		return "", ErrNilDBPointer
	}
	if len(data) == 0 {
		return "", ErrLength
	}

	for _, v := range data {
		if v == "" {
			return "", ErrMissingValue
		}
	}

	// Logic
	args := make([]interface{}, 0, len(data)*2)
	for k, v := range data {
		args = append(args, k, v)
	}

	result, err = r.db.MSet(args...).Result()
	if err != nil {
		return "", fmt.Errorf("Function Mset, return error:<%w>", err)
	}

	return result, nil
}

// Recieve multiple values. Return values and error.
//
// Params:
//
//	keys - array of name keys.
func (r *objectRedis) RecvMultipleStrings(keys []string) (values []string, err error) {

	// Check
	if r.db == nil {
		return nil, ErrNilDBPointer
	}
	if len(keys) == 0 {
		return nil, ErrLength
	}

	// Logic
	interfaceValues, err := r.db.MGet(keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("Function MGet, returned error:<%w>", err)
	}

	values = make([]string, len(interfaceValues))
	for i, v := range interfaceValues {
		if v != nil {
			values[i] = v.(string)
		} else {
			values[i] = ""
		}
	}

	return values, nil
}

// Delete string by key. Return error.
//
// Params:
//
//	key - key of string.
func (r *objectRedis) DeleteString(key string) error {

	// Check
	if r.db == nil {
		return ErrNilDBPointer
	}
	if key == "" {
		return ErrMissingKey
	}

	// Logic
	result, err := r.db.Del(key).Result()
	if err != nil {
		return fmt.Errorf("error while deleting key: %w", err)
	}

	if result == 0 {
		return fmt.Errorf("key does not exist")
	}

	return nil
}

// Delete multiple strings. Return error/
//
// Params:
//
//	keys - names of keys fo delete.
func (r *objectRedis) DeleteMultipleStrings(keys []string) error {

	// Check.
	if r.db == nil {
		return ErrNilDBPointer
	}
	if len(keys) == 0 {
		return ErrLength
	}
	for i, v := range keys {
		if v == "" {
			return fmt.Errorf("missing value by index <%d>", i)
		}
	}

	// Logic.
	result, err := r.db.Del(keys...).Result()
	if err != nil {
		return fmt.Errorf("error while deleting keys: %w", err)
	}

	if result == 0 {
		return fmt.Errorf("none of the keys exist")
	}

	return nil
}

// Send new integer with TTL. Return error.
//
// Params:
//
//	r - key.
//	v - value.
//	ttl - TTL.
func (r *objectRedis) SendIntegerTTL(k string, v int64, ttl time.Duration) (err error) {

	// Check.
	if r.db == nil {
		return ErrNilDBPointer
	}
	if k == "" {
		return ErrMissingKey
	}
	if ttl <= 0 {
		return ErrMissingTTL
	}

	// Logic.
	err = r.db.SetNX(k, v, ttl).Err()
	if err != nil {
		return fmt.Errorf("fault Tx data: %v", err)
	}
	return nil
}

// Increment integer value. Return error.
//
// Params:
//
//	r - key.
func (r *objectRedis) IncrementInteger(k string) (result int64, err error) {

	// Check.
	if r.db == nil {
		return 0, ErrNilDBPointer
	}
	if k == "" {
		return 0, ErrMissingKey
	}

	// Logic.
	result, err = r.db.Incr(k).Result()
	if err != nil {
		return 0, fmt.Errorf("error while incrementing key %s: %w", k, err)
	}

	return result, nil
}

// Add integer value. Returns new value and error.
//
// Params:
//
//	r - key.
//	v - value.
func (r *objectRedis) AddInteger(k string, v int64) (result int64, err error) {

	// Check.
	if r.db == nil {
		return 0, ErrNilDBPointer
	}
	if k == "" {
		return 0, ErrMissingKey
	}

	// Logic.
	result, err = r.db.IncrBy(k, v).Result()
	if err != nil {
		return 0, fmt.Errorf("error while add value key %s: %w", k, err)
	}

	return result, nil
}
