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
)
