package db

import (
	"fmt"
)

// Create new list. Return error.
//
// Params:
//
//	key - name of list.
//	values - adding info.
func (r *objectRedis) ListStringNew(key string, values []string) (err error) {

	// Check
	if r.db == nil {
		return ErrNilDBPointer
	}
	if key == "" {
		return ErrMissingKey
	}
	if values == nil {
		return ErrMissingValue
	}
	if len(values) == 0 {
		return ErrMissingValue
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res != 0 {
		return ErrKeyIsExists
	}

	// Logic
	err = r.db.LPush(key, values).Err()
	if err != nil {
		return fmt.Errorf("Function LPushX, return error: <%w>", err)
	}

	return nil
}

// Add to the left side exists list. Return error.
//
// Params:
//
//	key - name of list.
//	values - adding info.
func (r *objectRedis) ListStringAddLeft(key string, values []string) (err error) {

	// Check
	if r.db == nil {
		return ErrNilDBPointer
	}
	if key == "" {
		return ErrMissingKey
	}
	if values == nil {
		return ErrMissingValue
	}
	if len(values) == 0 {
		return ErrMissingValue
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return ErrKeyIsNotExists
	}

	// Logic
	_, err = r.db.LPush(key, values).Result()
	if err != nil {
		return fmt.Errorf("Function LPush, return error: <%w>", err)
	}

	return nil
}

// Add to the right side exists list. Return error.
//
// Params:
//
//	key - name of list.
//	values - adding info.
func (r *objectRedis) ListStringAddRight(key string, values []string) (err error) {

	// Check
	if r.db == nil {
		return ErrNilDBPointer
	}
	if key == "" {
		return ErrMissingKey
	}
	if values == nil {
		return ErrMissingValue
	}
	if len(values) == 0 {
		return ErrMissingValue
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return ErrKeyIsNotExists
	}

	// Logic
	_, err = r.db.RPush(key, values).Result()
	if err != nil {
		return fmt.Errorf("Function RPush, return error: <%w>", err)
	}

	return nil
}

// Recieve value from left side exists list. Returns value and error.
//
// Params:
//
//	key - name of list.
func (r *objectRedis) ListStringRecvLeft(key string) (value string, err error) {

	// Check
	if r.db == nil {
		return "", ErrNilDBPointer
	}
	if key == "" {
		return "", ErrMissingKey
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return "", fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return "", ErrKeyIsNotExists
	}

	// Logic
	value, err = r.db.LPop(key).Result()
	if err != nil {
		return "", fmt.Errorf("Function LPop, return error: <%w>", err)
	}

	return value, nil
}

// Recieve value from right side exists list. Returns value and error.
//
// Params:
//
//	key - name of list.
func (r *objectRedis) ListStringRecvRight(key string) (value string, err error) {

	// Check
	if r.db == nil {
		return "", ErrNilDBPointer
	}
	if key == "" {
		return "", ErrMissingKey
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return "", fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return "", ErrKeyIsNotExists
	}

	// Logic
	value, err = r.db.RPop(key).Result()
	if err != nil {
		return "", fmt.Errorf("Function LPop, return error: <%w>", err)
	}

	return value, nil
}

// Recieve len of exists list. Returns value and error.
//
// Params:
//
//	key - name of list.
func (r *objectRedis) ListStringRecvLen(key string) (value int64, err error) {

	// Check
	if r.db == nil {
		return 0, ErrNilDBPointer
	}
	if key == "" {
		return 0, ErrMissingKey
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return 0, fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return 0, ErrKeyIsNotExists
	}

	// Logic
	value, err = r.db.LLen(key).Result()
	if err != nil {
		return 0, fmt.Errorf("Function LLen, return error: <%w>", err)
	}

	return value, nil
}

// Move string form source list to target list. Returns moved volume and error.
//
// Params:
//
//	srcKey - source key of list.
//	destKey - destination key of list.
//	value - name of value
func (r *objectRedis) ListStringMoveByNameToNewLeft(srcKey, destKey string, value string) (int64, error) {

	// Check
	if r.db == nil {
		return 0, ErrNilDBPointer
	}
	if srcKey == "" || destKey == "" {
		return 0, ErrMissingKey
	}
	if value == "" {
		return 0, ErrMissingValue
	}

	exists, err := r.db.Exists(srcKey).Result()
	if err != nil {
		return 0, fmt.Errorf("Function Exists src, return error: <%w>", err)
	}
	if exists == 0 {
		return 0, ErrKeyIsNotExists
	}

	exists, err = r.db.Exists(destKey).Result()
	if err != nil {
		return 0, fmt.Errorf("Function Exists dest, return error: <%w>", err)
	}
	if exists != 0 {
		return 0, ErrKeyIsExists
	}

	// Logic
	removed, err := r.db.LRem(srcKey, 1, value).Result()
	if err != nil {
		return 0, fmt.Errorf("error removing value from source list: <%w>", err)
	}
	if removed == 0 {
		return 0, ErrValueIsNotExists
	}

	err = r.db.LPush(destKey, value).Err()
	if err != nil {
		return 0, fmt.Errorf("error adding value to destination list: <%w>", err)
	}

	return removed, nil
}

// Recieve range values from exists list. Returns values and error.
//
// Params:
//
//	key - name of list.
//	indStart - start index.
//	indStop - stop index.
func (r *objectRedis) ListStringRecvRange(key string, indStart, indStop int64) (values []string, err error) {

	// Check
	if r.db == nil {
		return nil, ErrNilDBPointer
	}
	if key == "" {
		return nil, ErrMissingKey
	}
	if indStart == indStop {
		return nil, ErrIndex
	}
	if indStart >= indStop {
		return nil, ErrIndex
	}
	if indStart < 0 || indStop < 0 {
		return nil, ErrIndex
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return nil, fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return nil, ErrKeyIsNotExists
	}

	size, err := r.db.LLen(key).Result()
	if err != nil {
		return nil, fmt.Errorf("Function LLen, return error: <%w>", err)
	}
	if indStop+1 > size {
		return nil, ErrIndexStopOver
	}

	// Logic
	values, err = r.db.LRange(key, indStart, indStop).Result()
	if err != nil {
		return nil, fmt.Errorf("Function LRange, return error: <%w>", err)
	}

	return values, nil
}

// Trim range values from exists list. Returns result and error.
//
// Params:
//
//	key - name of list.
//	indStart - start index.
//	indStop - stop index.
func (r *objectRedis) ListStringTrimRange(key string, indStart, indStop int64) (result string, err error) {

	// Check
	if r.db == nil {
		return "", ErrNilDBPointer
	}
	if key == "" {
		return "", ErrMissingKey
	}
	if indStart == indStop {
		return "", ErrIndex
	}
	if indStart < 0 {
		return "", ErrIndex
	}

	res, err := r.db.Exists(key).Result()
	if err != nil {
		return "", fmt.Errorf("Function Exists, return error: <%w>", err)
	}
	if res == 0 {
		return "", ErrKeyIsNotExists
	}

	size, err := r.db.LLen(key).Result()
	if err != nil {
		return "", fmt.Errorf("Function LLen, return error: <%w>", err)
	}
	if indStop+1 > size {
		return "", ErrIndexStopOver
	}

	// Logic
	result, err = r.db.LTrim(key, indStart, indStop).Result()
	if err != nil {
		return "", fmt.Errorf("Function LTrim, return error: <%w>", err)
	}

	return result, nil
}
