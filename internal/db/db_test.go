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

		key := "Foo-2"
		value := "Bar-2"
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

		key := "Foo98"
		value := "Bar98"
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

// Test SendMultipleStrings
func TestSendMultipleStrings(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Empty map", func(t *testing.T) {

		info := make(map[string]string)

		_, err := db.SendMultipleStrings(info)
		require.Equalf(t, ErrLength, err, "Errors is not equal")
	})

	t.Run("Missing value", func(t *testing.T) {

		info := make(map[string]string)
		info["Foo"] = ""

		_, err := db.SendMultipleStrings(info)
		require.Equalf(t, ErrMissingValue, err, "Errors is not equal")
	})

	t.Run("Correct data", func(t *testing.T) {

		info := make(map[string]string)
		info["A1"] = "B1"
		info["A2"] = "B2"

		_, err := db.SendMultipleStrings(info)
		require.NoErrorf(t, err, "Unexpected error")
	})
}

// Test RecvMultipleStrings
func TestRecvMultipleStrings(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Empty array", func(t *testing.T) {

		data := make([]string, 0)

		_, err := db.RecvMultipleStrings(data)
		require.Equalf(t, ErrLength, err, "Error is not equal")
	})

	t.Run("Missing data", func(t *testing.T) {

		data := []string{"AAA100", "BBB100"}

		rxData, err := db.RecvMultipleStrings(data)
		require.NoErrorf(t, err, "Unexpected error")
		assert.Equalf(t, len(data), len(rxData), "Not equal length")

		for k, v := range rxData {
			assert.Equalf(t, "", v, "Value by index <%d> not equal", k)
		}
	})

	t.Run("Correct data", func(t *testing.T) {

		// Send data.
		data := make(map[string]string)
		data["AAA"] = "AAA1"
		data["BBB"] = "BBB1"

		_, err := db.SendMultipleStrings(data)
		require.NoErrorf(t, err, "Fault send data")

		// Recieve data.
		reqData := make([]string, 0, len(data))
		for _, v := range data {
			reqData = append(reqData, v)
		}

		respData, err := db.RecvMultipleStrings(reqData)
		require.NoErrorf(t, err, "Unexpected recieve error")
		assert.Equalf(t, len(reqData), len(respData), "Not equal length")

		for i, v := range reqData {
			assert.Equalf(t, reqData[i], v, "Not equal value by index <%d>", i)
		}

	})

	t.Run("Add missing key", func(t *testing.T) {

		// Send data.
		data := make(map[string]string)
		data["AAA"] = "AAA1"
		data["BBB"] = "BBB1"

		_, err := db.SendMultipleStrings(data)
		require.NoErrorf(t, err, "Fault send data")

		// Recieve data.
		reqData := make([]string, 0)
		for _, v := range data {
			reqData = append(reqData, v)
		}
		reqData = append(reqData, "CCC")

		respData, err := db.RecvMultipleStrings(reqData)
		require.NoErrorf(t, err, "Unexpected recieve error")
		assert.Equalf(t, len(reqData), len(respData), "Not equal length")

		assert.Equalf(t, data[reqData[0]], respData[0], "Value by index <0> is not equals")
		assert.Equalf(t, data[reqData[1]], respData[1], "Value by index <1> is not equals")
		assert.Equalf(t, "", respData[2], "Value by index <2> is not equals")

	})
}

// Test DeleteString
func TestDeleteString(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing Key", func(t *testing.T) {

		err := db.DeleteString("")
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Correct data", func(t *testing.T) {

		key := "Foo2"
		value := "Bar2"
		ttl := time.Duration(1 * time.Second)

		err := db.SendStringTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error send")

		rxData, err := db.RecvString(key)
		require.NoErrorf(t, err, "Unexpected error recieve")
		assert.Equalf(t, value, rxData, "Values is not equals")

		// Test
		err = db.DeleteString(key)
		require.NoErrorf(t, err, "Unexpected error delete")

		rxData, err = db.RecvString(key)
		require.Errorf(t, err, "Want error")
		assert.Equalf(t, "", rxData, "Values is not equals")
	})

	t.Run("Unexists key", func(t *testing.T) {

		key := "Foo22"

		// Test
		err = db.DeleteString(key)
		require.Errorf(t, err, "Want error")

	})
}

// Test DeleteString
func TestDeleteMultipleStrings(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing Keys", func(t *testing.T) {

		keys := make([]string, 0)

		err := db.DeleteMultipleStrings(keys)
		require.Equalf(t, ErrLength, err, "Error is not equal")
	})

	t.Run("Correct data", func(t *testing.T) {

		txData := make(map[string]string)
		txData["Test1"] = "Data1"
		txData["Test2"] = "Data2"

		_, err := db.SendMultipleStrings(txData)
		require.NoErrorf(t, err, "Unexpected error send multiple")

		// Test
		delKeys := make([]string, 0, len(txData))
		for k, v := range txData {
			delKeys = append(delKeys, k)
			_ = v
		}

		err = db.DeleteMultipleStrings(delKeys)
		require.NoErrorf(t, err, "Unexpected delete multiple")

		rxData, err := db.RecvMultipleStrings(delKeys)
		require.NoErrorf(t, err, "Unexpected multiple recieve")
		assert.Equalf(t, len(delKeys), len(rxData), "Sizes is not equal")

	})

	t.Run("Add missing key", func(t *testing.T) {

		txData := make(map[string]string)
		txData["Test1"] = "Data1"
		txData["Test2"] = "Data2"

		_, err := db.SendMultipleStrings(txData)
		require.NoErrorf(t, err, "Unexpected error send multiple")

		// Test
		delKeys := make([]string, 0, len(txData))
		for k, v := range txData {
			delKeys = append(delKeys, k)
			_ = v
		}
		delKeys = append(delKeys, "Test3") // Missing value

		err = db.DeleteMultipleStrings(delKeys)
		require.NoErrorf(t, err, "Unexpected delete multiple")

		rxData, err := db.RecvMultipleStrings(delKeys)
		require.NoErrorf(t, err, "Unexpected multiple recieve")
		assert.Equalf(t, len(delKeys), len(rxData), "Sizes is not equal")

	})
}

// Test SendIntegerTTL
func TestSendIntegerTTL(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := int64(123)
		ttl := time.Duration(1 * time.Second)

		err := db.SendIntegerTTL(key, value, ttl)
		require.Equalf(t, ErrMissingKey, err, "Fault is not equal")
	})

	t.Run("Missing ttl", func(t *testing.T) {

		key := "Foo"
		value := int64(123)
		ttl := time.Duration(0 * time.Second)

		err := db.SendIntegerTTL(key, value, ttl)
		require.Equalf(t, ErrMissingTTL, err, "Fault is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		key := "Foo"
		value := int64(123)
		ttl := time.Duration(1 * time.Second)

		err := db.SendIntegerTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error")
	})

}

// Test IncrementInteger
func TestIncrementInteger(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		_, err := db.IncrementInteger("")
		require.Equalf(t, ErrMissingKey, err, "Errors is not equals")
	})

	t.Run("Key is not exists", func(t *testing.T) {

		key := "ABC"

		result, err := db.IncrementInteger(key)
		require.Nilf(t, err, "Want nil")
		assert.Equalf(t, int64(1), result, "Result is not equal")

		err = db.DeleteString(key)
		require.NoErrorf(t, err, "Enexpected error delete")
	})

	t.Run("Key exists", func(t *testing.T) {

		key := "Foo454545"
		value := int64(123)
		ttl := time.Duration(1 * time.Second)

		err := db.SendIntegerTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error send")

		rxValue, err := db.IncrementInteger(key)
		require.NoErrorf(t, err, "Unexpected error increment")
		assert.Equalf(t, int64(124), rxValue, "Values is not equals")

	})
}

// Test AddInteger
func TestAddInteger(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := int64(2)

		_, err := db.AddInteger(key, value)
		require.Equalf(t, ErrMissingKey, err, "Errors is not equals")
	})

	t.Run("Key is not exists", func(t *testing.T) {

		key := "ABC"
		val := int64(2)

		result, err := db.AddInteger(key, val)
		require.Nilf(t, err, "Want nil")
		assert.Equalf(t, int64(2), result, "Result is not equal")

		err = db.DeleteString(key)
		require.NoErrorf(t, err, "Enexpected error delete")
	})

	t.Run("Key exists", func(t *testing.T) {

		key := "Foo5"
		value := int64(2)
		ttl := time.Duration(1 * time.Second)

		err := db.SendIntegerTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error send")

		rxValue, err := db.AddInteger(key, value)
		require.NoErrorf(t, err, "Unexpected error increment")
		assert.Equalf(t, int64(4), rxValue, "Values is not equals")

	})
}
