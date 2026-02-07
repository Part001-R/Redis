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
)
