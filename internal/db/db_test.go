package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test constructor.
func TestNew(t *testing.T) {

	t.Run("Missing DB address", func(t *testing.T) {

		addr := ""
		pwd := ""
		numbDB := 0

		inst, err := New(addr, pwd, numbDB)
		require.Equalf(t, ErrNilDBPointer, err, "Not equal error")
		assert.Nilf(t, inst, "Want nil on pointer")
	})

	t.Run("DB numb less <0>", func(t *testing.T) {

		addr := "localhost:6379"
		pwd := ""
		numbDB := -1

		inst, err := New(addr, pwd, numbDB)
		require.Equalf(t, "number of db less 0:<-1>", err.Error(), "Not equal error")
		assert.Nilf(t, inst, "Want nil on pointer")
	})

	t.Run("Correct DB connect", func(t *testing.T) {

		addr := "localhost:6379"
		pwd := ""
		numbDB := 0

		inst, err := New(addr, pwd, numbDB)
		require.NoErrorf(t, err, "Wait not error")
		require.NotNilf(t, inst, "Want pointer")

		err = inst.Close()
		assert.NoErrorf(t, err, "Unexpected error DB close connect")

	})
}

// Test send string with TTL.
func TestSendStringTTL(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := "Foo"
		ttl := time.Duration(1 * time.Second)

		err := db.SendStringTTL(key, value, ttl)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Missing value", func(t *testing.T) {

		key := "Foo"
		value := ""
		ttl := time.Duration(1 * time.Second)

		err := db.SendStringTTL(key, value, ttl)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Missing TTL", func(t *testing.T) {

		key := "Foo"
		value := "Bar"
		ttl := time.Duration(0 * time.Second)

		err := db.SendStringTTL(key, value, ttl)
		require.Equalf(t, ErrMissingTTL, err, "Error is not equal")
	})

	t.Run("Correct send", func(t *testing.T) {

		key := "Foo"
		value := "Bar"
		ttl := time.Duration(1 * time.Second)

		err := db.SendStringTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error")

		rxValue, err := db.RecvString(key)
		require.NoErrorf(t, err, "Unexpected recieve value")
		assert.Equalf(t, value, rxValue, "Values is not equal")

		time.Sleep(2 * time.Second)

		rxValue, err = db.RecvString(key)
		require.Errorf(t, err, "Want error")
	})
}

// Test recieve string by key.
func TestRecvString(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		rxValue, err := db.RecvString("")
		require.Errorf(t, err, "Want error")
		assert.Equalf(t, "", rxValue, "Values is not equal")
	})

	t.Run("Correct recieve", func(t *testing.T) {

		key := "FooA"
		value := "BarA"
		ttl := time.Duration(1 * time.Second)

		err := db.SendStringTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error")

		rxValue, err := db.RecvString(key)
		require.NoErrorf(t, err, "Unexpected recieve value")
		assert.Equalf(t, value, rxValue, "Values is not equal")

	})
}

// Test exixts key.
func TestCheckExistsKey(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		rxValue, err := db.CheckExistsKey("")
		require.Equalf(t, ErrMissingKey, err, "Errors is not equal")
		assert.Equalf(t, int64(0), rxValue, "Values is not equal")
	})

	t.Run("Correct request", func(t *testing.T) {

		key := "Foo"
		value := "Bar"
		ttl := time.Duration(1 * time.Second)

		err := db.SendStringTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error")

		rxValue, err := db.CheckExistsKey(key)
		require.NoErrorf(t, err, "Unexpected error")
		assert.Equalf(t, int64(1), rxValue, "Values is not equal")

		time.Sleep(1100 * time.Millisecond)

		rxValue, err = db.CheckExistsKey(key)
		require.NoErrorf(t, err, "Unexpected error")
		assert.Equalf(t, int64(0), rxValue, "Value is not equal")
	})

}

// Test update exists string.
func TestUpdateStringTTL(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := "Bar"
		ttl := time.Duration(1 * time.Second)

		err := db.UpdateStringTTL(key, value, ttl)
		require.Equalf(t, ErrMissingKey, err, "Errir is not equal")
	})

	t.Run("Missing value", func(t *testing.T) {

		key := "Foo"
		value := ""
		ttl := time.Duration(1 * time.Second)

		err := db.UpdateStringTTL(key, value, ttl)
		require.Equalf(t, ErrMissingValue, err, "Errir is not equal")
	})

	t.Run("Missing ttl", func(t *testing.T) {

		key := "Foo"
		value := "Bar"
		ttl := time.Duration(0 * time.Second)

		err := db.UpdateStringTTL(key, value, ttl)
		require.Equalf(t, ErrMissingTTL, err, "Errir is not equal")
	})

	t.Run("Try update missing entry", func(t *testing.T) {

		key := "Foo"
		value := "Bar"
		ttl := time.Duration(1 * time.Second)

		err := db.UpdateStringTTL(key, value, ttl)
		require.Nilf(t, err, "Want nil")
	})

	t.Run("Correct update", func(t *testing.T) {

		key := "Foo"
		value := "Bar"
		ttl := time.Duration(1 * time.Second)
		err := db.SendStringTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected send error")

		value += "Bar"
		err = db.UpdateStringTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected update error")

		rxValue, err := db.RecvString(key)
		require.NoErrorf(t, err, "unexpected error recieve value")
		assert.Equalf(t, value, rxValue, "Values is not equal")
	})
}

// Test update exists string with get prev value.
func TestUpdateStringRecievePrevTTL(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := "Bar"
		_, err := db.UpdateStringRecievePrev(key, value)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Missing value", func(t *testing.T) {

		key := "Foo"
		value := ""
		_, err := db.UpdateStringRecievePrev(key, value)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Not exists update", func(t *testing.T) {

		key := "Foo"
		value := "Bar"

		_, err := db.UpdateStringRecievePrev(key, value)
		require.Nilf(t, err, "want nil")
	})

	t.Run("Exists update", func(t *testing.T) {

		key := "Foo"
		value := "Bar"
		ttl := time.Duration(1 * time.Second)

		err := db.SendStringTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error")

		value2 := value + value
		rxValue, err := db.UpdateStringRecievePrev(key, value2)
		require.NotNilf(t, rxValue, "unexpected nil")
		assert.Equalf(t, value, rxValue, "Values is not equal")

		value3 := value2 + value
		rxValue, err = db.UpdateStringRecievePrev(key, value3)
		require.NotNilf(t, rxValue, "unexpected nil")
		assert.Equalf(t, value2, rxValue, "Values is not equal")
	})
}
