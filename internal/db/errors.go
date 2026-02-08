package db

import "errors"

var (
	// nil DB pointer
	ErrNilDBPointer = errors.New("nil DB pointer")

	// missing key
	ErrMissingKey = errors.New("missing key")

	// missing value
	ErrMissingValue = errors.New("missing value")

	// missing TTL
	ErrMissingTTL = errors.New("missing TTL")

	// fault update value
	ErrUpdateValue = errors.New("fault update data: redis: nil")

	// error length
	ErrLength = errors.New("error length")

	// is not integer
	ErrIsNotInteger = errors.New("is not integer")

	// key is not exists
	ErrKeyIsNotExists = errors.New("key is not exists")

	// value is not exists
	ErrValueIsNotExists = errors.New("value is not exists")

	// key is exists
	ErrKeyIsExists = errors.New("key is exists")

	// error index
	ErrIndex = errors.New("error index")

	// index stop is over size
	ErrIndexStopOver = errors.New("index stop is over size")
)
