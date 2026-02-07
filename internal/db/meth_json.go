package db

import (
	"encoding/json"
	"fmt"
	"time"
)

// Send new JSON. Return error.
//
// Params:
//
//	key - key of entry.
//	value - value of entry.
//	ttl - ttl time of entry.
func (r *objectRedis) SendNewJSONTTL(key string, value TestJSON, ttl time.Duration) error {

	// Check
	if r.db == nil {
		return ErrNilDBPointer
	}
	if key == "" {
		return ErrMissingKey
	}
	if ttl <= 0 {
		return ErrMissingTTL
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res != 0 {
		return ErrKeyIsExists
	}

	// Logic
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("Function Marshal, return error <%w>", err)
	}

	if err := r.db.SetNX(key, data, ttl).Err(); err != nil {
		return fmt.Errorf("Function SetNX, return error <%w>", err)
	}

	return nil
}

// Recieve JSON. Returns data and error.
//
// Params:
//
//	key - key of entry.
func (r *objectRedis) RecvJSON(key string) (TestJSON, error) {

	// Check
	if r.db == nil {
		return TestJSON{}, ErrNilDBPointer
	}
	if key == "" {
		return TestJSON{}, ErrMissingKey
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return TestJSON{}, fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return TestJSON{}, ErrKeyIsNotExists
	}

	// Logic
	data, err := r.db.Get(key).Result()
	if err != nil {
		return TestJSON{}, fmt.Errorf("Function Get, return error: <%w>", err)
	}

	var value TestJSON
	if err := json.Unmarshal([]byte(data), &value); err != nil {
		return TestJSON{}, fmt.Errorf("Error unmarshalling JSON: <%w>", err)
	}

	return value, nil
}

// Update JSON. Return error.
//
// Params:
//
//	key - key of entry.
//	value - value of entry.
//	ttl - ttl time of entry.
func (r *objectRedis) UpdateJSONTTL(key string, value TestJSON, ttl time.Duration) error {

	// Check
	if r.db == nil {
		return ErrNilDBPointer
	}
	if key == "" {
		return ErrMissingKey
	}
	if ttl <= 0 {
		return ErrMissingTTL
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return ErrKeyIsNotExists
	}

	// Logic
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("Function Marshal, return error <%w>", err)
	}

	if err := r.db.SetXX(key, data, ttl).Err(); err != nil {
		return fmt.Errorf("Function SetNX, return error <%w>", err)
	}

	return nil
}
