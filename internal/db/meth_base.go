package db

import (
	"fmt"
	"time"
)

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
	if k == "" {
		return "", ErrMissingKey
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

	rxValue, err := r.db.Exists(k).Result()
	if err != nil {
		return fmt.Errorf("function Exists, return error <%w>", err)
	}
	if rxValue == 0 {
		return ErrKeyIsNotExists
	}

	// Logic.
	err = r.db.SetXX(k, v, ttl).Err()
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

	rxValue, err := r.db.Exists(k).Result()
	if err != nil {
		return "", fmt.Errorf("function Exists, return error <%w>", err)
	}
	if rxValue == 0 {
		return "", ErrKeyIsNotExists
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
func (r *objectRedis) Delete(key string) error {

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

// Increment exists integer value. Return error.
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

	ex, err := r.db.Exists(k).Result()
	if err != nil {
		return 0, fmt.Errorf("function Exists, return error <%w>", err)
	}
	if ex == 0 {
		return 0, ErrKeyIsNotExists
	}

	// Logic.
	result, err = r.db.Incr(k).Result()
	if err != nil {
		return 0, fmt.Errorf("error while incrementing key %s: %w", k, err)
	}

	return result, nil
}

// Add exists integer value. Returns new value and error.
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

	ex, err := r.db.Exists(k).Result()
	if err != nil {
		return 0, fmt.Errorf("function Exists, return error <%w>", err)
	}
	if ex == 0 {
		return 0, ErrKeyIsNotExists
	}

	// Logic.
	result, err = r.db.IncrBy(k, v).Result()
	if err != nil {
		return 0, fmt.Errorf("error while add value key %s: %w", k, err)
	}

	return result, nil
}

// Decrement exists integer value. Return error.
//
// Params:
//
//	r - key.
func (r *objectRedis) DecrementInteger(k string) (result int64, err error) {

	// Check.
	if r.db == nil {
		return 0, ErrNilDBPointer
	}
	if k == "" {
		return 0, ErrMissingKey
	}

	ex, err := r.db.Exists(k).Result()
	if err != nil {
		return 0, fmt.Errorf("function Exists, return error <%w>", err)
	}
	if ex == 0 {
		return 0, ErrKeyIsNotExists
	}

	// Logic.
	result, err = r.db.Decr(k).Result()
	if err != nil {
		return 0, fmt.Errorf("error <%w> while decrement key <%s>", err, k)
	}

	return result, nil
}

// Subtraction exists integer value. Returns new value and error.
//
// Params:
//
//	r - key.
//	v - value.
func (r *objectRedis) SubInteger(k string, v int64) (result int64, err error) {

	// Check.
	if r.db == nil {
		return 0, ErrNilDBPointer
	}
	if k == "" {
		return 0, ErrMissingKey
	}

	ex, err := r.db.Exists(k).Result()
	if err != nil {
		return 0, fmt.Errorf("function Exists, return error <%w>", err)
	}
	if ex == 0 {
		return 0, ErrKeyIsNotExists
	}

	// Logic.
	result, err = r.db.DecrBy(k, v).Result()
	if err != nil {
		return 0, fmt.Errorf("error while add value key %s: %w", k, err)
	}

	return result, nil
}

// Send new float with TTL. Return error.
//
// Params:
//
//	r - key.
//	v - value.
//	ttl - TTL.
func (r *objectRedis) SendFloatTTL(k string, v float64, ttl time.Duration) (err error) {

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

	result, err := r.db.Exists(k).Result()
	if err != nil {
		return fmt.Errorf("function Exists, return error:<%w>", err)
	}
	if result != 0 {
		return ErrKeyIsExists
	}

	// Logic.
	err = r.db.SetNX(k, v, ttl).Err()
	if err != nil {
		return fmt.Errorf("fault Tx data: %v", err)
	}
	return nil
}

// Adding exists float value. Returns new value and error.
//
// Params:
//
//	r - key.
//	v - value.
func (r *objectRedis) AddFloat(k string, v float64) (result float64, err error) {

	// Check.
	if r.db == nil {
		return 0, ErrNilDBPointer
	}
	if k == "" {
		return 0, ErrMissingKey
	}

	ex, err := r.db.Exists(k).Result()
	if err != nil {
		return 0, fmt.Errorf("function Exists, return error <%w>", err)
	}
	if ex == 0 {
		return 0, ErrKeyIsNotExists
	}

	// Logic.
	result, err = r.db.IncrByFloat(k, v).Result()
	if err != nil {
		return 0, fmt.Errorf("error while add value key %s: %w", k, err)
	}

	return result, nil
}
