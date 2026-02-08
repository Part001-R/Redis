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

		_, err := db.RecvString("")
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
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

	t.Run("Not exists", func(t *testing.T) {

		key := "Foo989"

		result, err := db.CheckExistsKey(key)
		require.NoErrorf(t, err, "Unexpected error")
		assert.Equalf(t, int64(0), result, "Value is not equal")

	})

	t.Run("Exists", func(t *testing.T) {

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

		key := "Fooo"
		value := "Bar"
		ttl := time.Duration(1 * time.Second)

		err := db.UpdateStringTTL(key, value, ttl)
		require.Equalf(t, ErrKeyIsNotExists, err, "Error is not equal")
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

		err := db.Delete("")
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
		err = db.Delete(key)
		require.NoErrorf(t, err, "Unexpected error delete")

		rxData, err = db.RecvString(key)
		require.Errorf(t, err, "Want error")
		assert.Equalf(t, "", rxData, "Values is not equals")
	})

	t.Run("Unexists key", func(t *testing.T) {

		key := "Foo22"

		// Test
		err = db.Delete(key)
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

		_, err := db.IncrementInteger(key)
		assert.Equalf(t, ErrKeyIsNotExists, err, "Result is not equal")

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

		_, err := db.AddInteger(key, val)
		assert.Equalf(t, ErrKeyIsNotExists, err, "Error is not exists")
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

// Test DecrementInteger
func TestDecrementInteger(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		_, err := db.DecrementInteger("")
		require.Equalf(t, ErrMissingKey, err, "Errors is not equals")
	})

	t.Run("Key is not exists", func(t *testing.T) {

		key := "ABC"

		_, err := db.DecrementInteger(key)
		assert.Equalf(t, ErrKeyIsNotExists, err, "Result is not equal")

	})

	t.Run("Key exists", func(t *testing.T) {

		key := "Foo3434"
		value := int64(123)
		ttl := time.Duration(1 * time.Second)

		err := db.SendIntegerTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error send")

		rxValue, err := db.DecrementInteger(key)
		require.NoErrorf(t, err, "Unexpected error decrement")
		assert.Equalf(t, int64(122), rxValue, "Values is not equals")

		err = db.Delete(key)
		require.NoErrorf(t, err, "Unexpected error delete")

	})
}

// Test SubInteger
func TestSubInteger(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := int64(2)

		_, err := db.SubInteger(key, value)
		require.Equalf(t, ErrMissingKey, err, "Errors is not equals")
	})

	t.Run("Key is not exists", func(t *testing.T) {

		key := "ABC"
		val := int64(2)

		_, err := db.SubInteger(key, val)
		assert.Equalf(t, ErrKeyIsNotExists, err, "Error is not exists")
	})

	t.Run("Key exists", func(t *testing.T) {

		key := "Foo55"
		value := int64(3)
		ttl := time.Duration(1 * time.Second)

		err := db.SendIntegerTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error send")

		value = int64(1)

		rxValue, err := db.SubInteger(key, value)
		require.NoErrorf(t, err, "Unexpected error subtraction")
		assert.Equalf(t, int64(2), rxValue, "Values is not equals")

		err = db.Delete(key)
		require.NoErrorf(t, err, "Unexpected error delete")
	})
}

// Test SendfloatTTL
func TestSendfloatTTL(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := float64(123.4)
		ttl := time.Duration(1 * time.Second)

		err := db.SendFloatTTL(key, value, ttl)
		require.Equalf(t, ErrMissingKey, err, "Fault is not equal")
	})

	t.Run("Missing ttl", func(t *testing.T) {

		key := "Foo"
		value := float64(123.4)
		ttl := time.Duration(0 * time.Second)

		err := db.SendFloatTTL(key, value, ttl)
		require.Equalf(t, ErrMissingTTL, err, "Fault is not equal")
	})

	t.Run("New key", func(t *testing.T) {

		key := "Foo9"
		value := float64(123.4)
		ttl := time.Duration(1 * time.Second)

		err := db.SendFloatTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error send float")

		err = db.Delete(key)
		require.NoErrorf(t, err, "Unexpected error post delete")
	})

	t.Run("Exists key", func(t *testing.T) {

		key := "Foo9"
		value := float64(123.4)
		ttl := time.Duration(1 * time.Second)

		err := db.SendFloatTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error send float")

		err = db.SendFloatTTL(key, value, ttl)
		assert.Equalf(t, ErrKeyIsExists, err, "Error is not equal")

		err = db.Delete(key)
		require.NoErrorf(t, err, "Unexpected error post delete")
	})

}

// Test AddFloat
func TestAddFloat(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := float64(2.3)

		_, err := db.AddFloat(key, value)
		require.Equalf(t, ErrMissingKey, err, "Errors is not equals")
	})

	t.Run("Key is not exists", func(t *testing.T) {

		key := "ABC"
		value := float64(2.3)

		_, err := db.AddFloat(key, value)
		assert.Equalf(t, ErrKeyIsNotExists, err, "Error is not exists")
	})

	t.Run("Key exists", func(t *testing.T) {

		key := "Foo9"
		value := float64(2.3)
		ttl := time.Duration(1 * time.Second)

		err := db.SendFloatTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error send")

		rxValue, err := db.AddFloat(key, value)
		require.NoErrorf(t, err, "Unexpected error increment")
		assert.Equalf(t, float64(4.6), rxValue, "Values is not equals")

		err = db.Delete(key)
		require.NoErrorf(t, err, "Unexpected error delete")

	})
}

// ===============================
// ===                         ===
// ===         JSON            ===
// ===                         ===
// ===============================

// Test SendNewJSONTTL
func TestSendNewJSONTTL(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := TestJSON{
			AAA: "A-1",
			BBB: "B-1",
			CCC: "C-1",
		}
		ttl := 1 * time.Second

		err := db.SendNewJSONTTL(key, value, ttl)
		assert.Equalf(t, ErrMissingKey, err, "Is not equal error")
	})

	t.Run("new key", func(t *testing.T) {

		key := "Foo7"
		value := TestJSON{
			AAA: "A-1",
			BBB: "B-1",
			CCC: "C-1",
		}
		ttl := 1 * time.Second

		err := db.SendNewJSONTTL(key, value, ttl)
		assert.NoErrorf(t, err, "Unexpected error send JSON")

		err = db.Delete(key)
		assert.NoErrorf(t, err, "Unexpected error delete")
	})

	t.Run("exists key", func(t *testing.T) {

		key := "Foo7"
		value := TestJSON{
			AAA: "A-1",
			BBB: "B-1",
			CCC: "C-1",
		}
		ttl := 1 * time.Second

		err := db.SendNewJSONTTL(key, value, ttl)
		assert.NoErrorf(t, err, "Unexpected error send JSON")

		err = db.SendNewJSONTTL(key, value, ttl)
		assert.Equalf(t, ErrKeyIsExists, err, "Error is not equal")

		err = db.Delete(key)
		assert.NoErrorf(t, err, "Unexpected error delete")
	})
}

// Test RecvJSON
func TestRecvJSON(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		_, err := db.RecvJSON("")
		assert.Equalf(t, ErrMissingKey, err, "Error is not equal")

	})

	t.Run("Key not exists", func(t *testing.T) {

		key := "key-1"

		_, err := db.RecvJSON(key)
		assert.Equalf(t, ErrKeyIsNotExists, err, "Error is not equal")

	})

	t.Run("Key exists", func(t *testing.T) {

		key := "key-1"
		value := TestJSON{
			AAA: "AA",
			BBB: "BB",
			CCC: "CC",
		}
		ttl := time.Duration(1 * time.Second)

		err := db.SendNewJSONTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error SendNewJSONTTL")

		rxData, err := db.RecvJSON(key)
		require.NoErrorf(t, err, "Unexpected error RecvJSON")
		assert.Equalf(t, value.AAA, rxData.AAA, "AAA is not equal")
		assert.Equalf(t, value.BBB, rxData.BBB, "BBB is not equal")
		assert.Equalf(t, value.CCC, rxData.CCC, "CCC is not equal")

		err = db.Delete(key)
		require.NoErrorf(t, err, "Unexpected error delete")

	})
}

// Test UpdateJSONTTL
func TestUpdateJSONTTL(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		value := TestJSON{
			AAA: "1",
			BBB: "2",
			CCC: "3",
		}
		ttl := time.Duration(1 * time.Second)

		err := db.UpdateJSONTTL(key, value, ttl)
		assert.Equalf(t, ErrMissingKey, err, "Error is not equal")

	})

	t.Run("Missing ttl", func(t *testing.T) {

		key := "Foo"
		value := TestJSON{
			AAA: "1",
			BBB: "2",
			CCC: "3",
		}
		ttl := time.Duration(0 * time.Second)

		err := db.UpdateJSONTTL(key, value, ttl)
		assert.Equalf(t, ErrMissingTTL, err, "Error is not equal")

	})

	t.Run("Not exists key", func(t *testing.T) {

		key := "Foo-1111111"
		value := TestJSON{
			AAA: "1",
			BBB: "2",
			CCC: "3",
		}
		ttl := time.Duration(1 * time.Second)

		err := db.UpdateJSONTTL(key, value, ttl)
		assert.Equalf(t, ErrKeyIsNotExists, err, "Error is not equal")

	})

	t.Run("Update", func(t *testing.T) {

		// Prepare
		key := "Foo-90"
		value := TestJSON{
			AAA: "1",
			BBB: "2",
			CCC: "3",
		}
		ttl := time.Duration(1 * time.Second)

		err := db.SendNewJSONTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error SendNewJSONTTL")

		defer func() {
			err := db.Delete(key)
			assert.NoErrorf(t, err, "Unexpected error DeleteString")
		}()

		// Test
		value = TestJSON{
			AAA: "11",
			BBB: "22",
			CCC: "33",
		}
		err = db.UpdateJSONTTL(key, value, ttl)
		require.NoErrorf(t, err, "Unexpected error UpdateJSONTTL")

		rxData, err := db.RecvJSON(key)
		require.NoErrorf(t, err, "Unexpected error RecvJSON")
		assert.Equalf(t, value.AAA, rxData.AAA, "AAA is not equal")
		assert.Equalf(t, value.BBB, rxData.BBB, "BBB is not equal")
		assert.Equalf(t, value.CCC, rxData.CCC, "CCC is not equal")

	})
}

// ===============================
// ===                         ===
// ===         List            ===
// ===                         ===
// ===============================

// Test SendNewListString
func TestSendNewListString(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		values := []string{"A", "B", "C"}

		err := db.ListStringNew(key, values)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Missing value", func(t *testing.T) {

		key := "List:1"

		err := db.ListStringNew(key, nil)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Empty value", func(t *testing.T) {

		key := "List:1"
		values := []string{}

		err := db.ListStringNew(key, values)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		key := "List:1"
		values := []string{"A"}

		err := db.ListStringNew(key, values)
		assert.NoErrorf(t, err, "Unexpected error send")

		err = db.Delete(key)
		assert.NoErrorf(t, err, "Unexpected error delete")
	})

	t.Run("Duplicat", func(t *testing.T) {

		key := "List:1"
		values := []string{"A"}

		err := db.ListStringNew(key, values)
		assert.NoErrorf(t, err, "Unexpected error send")

		err = db.ListStringNew(key, values)
		assert.Equalf(t, ErrKeyIsExists, err, "Error is not equal")

		err = db.Delete(key)
		assert.NoErrorf(t, err, "Unexpected error delete")
	})
}

// Test ListStringAddLeft
func TestListStringAddLeft(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		values := []string{"A", "B", "C"}

		err := db.ListStringAddLeft(key, values)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Missing value", func(t *testing.T) {

		key := "List:1"

		err := db.ListStringAddLeft(key, nil)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Empty value", func(t *testing.T) {

		key := "List:1"
		values := []string{}

		err := db.ListStringAddLeft(key, values)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		key := "List:1"
		values := []string{"A"}

		// Create
		err := db.ListStringNew(key, values)
		require.NoErrorf(t, err, "Unexpected error send")

		defer func() {
			err := db.Delete(key)
			assert.NoErrorf(t, err, "Unexpected error delete")
		}()

		// Add
		values = []string{"B"}

		err = db.ListStringAddLeft(key, values)
		assert.NoErrorf(t, err, "Unexpected error add")

	})
}

// Test ListStringAddRight
func TestListStringAddRight(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		values := []string{"A", "B", "C"}

		err := db.ListStringAddRight(key, values)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Missing value", func(t *testing.T) {

		key := "List:1"

		err := db.ListStringAddRight(key, nil)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Empty value", func(t *testing.T) {

		key := "List:1"
		values := []string{}

		err := db.ListStringAddRight(key, values)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		key := "List:1"
		values := []string{"A"}

		// Create
		err := db.ListStringNew(key, values)
		require.NoErrorf(t, err, "Unexpected error send")

		defer func() {
			err := db.Delete(key)
			assert.NoErrorf(t, err, "Unexpected error delete")
		}()

		// Add
		values = []string{"B"}

		err = db.ListStringAddRight(key, values)
		assert.NoErrorf(t, err, "Unexpected error add")

	})
}

// Test ListStringRecvLeft
func TestListStringRecvLeft(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""

		_, err := db.ListStringRecvLeft(key)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		// Prepare
		key := "List:1"
		values := []string{"A", "B", "C"}

		err := db.ListStringNew(key, values)
		require.NoErrorf(t, err, "Unexpected error send new")

		defer db.Delete(key)

		// Test
		rxValue, err := db.ListStringRecvLeft(key)
		require.NoErrorf(t, err, "Unexpected error recieve by index 0")
		assert.Equalf(t, values[2], rxValue, "Value by index 2 is not equal")

		rxValue, err = db.ListStringRecvLeft(key)
		require.NoErrorf(t, err, "Unexpected error recieve by index 0")
		assert.Equalf(t, values[1], rxValue, "Value by index 1 is not equal")

		rxValue, err = db.ListStringRecvLeft(key)
		require.NoErrorf(t, err, "Unexpected error recieve by index 0")
		assert.Equalf(t, values[0], rxValue, "Value by index 0 is not equal")

		rxValue, err = db.ListStringRecvLeft(key)
		require.Equalf(t, ErrKeyIsNotExists, err, "Error is not equal")

	})

}

// Test ListStringRecvRight
func TestListStringRecvRight(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""

		_, err := db.ListStringRecvRight(key)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		// Prepare
		key := "List:1"
		values := []string{"A", "B", "C"}

		err := db.ListStringNew(key, values)
		require.NoErrorf(t, err, "Unexpected error send new")

		defer db.Delete(key)

		values2 := []string{"D"}
		err = db.ListStringAddRight(key, values2)
		require.NoErrorf(t, err, "Unexpected error add right")

		// Test
		rxValue, err := db.ListStringRecvRight(key)
		require.NoErrorf(t, err, "Unexpected error recieve by index 0")
		assert.Equalf(t, values2[0], rxValue, "Value2 by index 0 is not equal")

		rxValue, err = db.ListStringRecvRight(key)
		require.NoErrorf(t, err, "Unexpected error recieve by index 0")
		assert.Equalf(t, values[0], rxValue, "Value by index 0 is not equal")

		rxValue, err = db.ListStringRecvRight(key)
		require.NoErrorf(t, err, "Unexpected error recieve by index 1")
		assert.Equalf(t, values[1], rxValue, "Value by index 1 is not equal")

		rxValue, err = db.ListStringRecvRight(key)
		require.NoErrorf(t, err, "Unexpected error recieve by index 2")
		assert.Equalf(t, values[2], rxValue, "Value by index 2 is not equal")

		rxValue, err = db.ListStringRecvRight(key)
		require.Equalf(t, ErrKeyIsNotExists, err, "Error is not equal")

	})

	t.Run("Dublicat", func(t *testing.T) {

		// Prepare
		key := "List:1"
		values := []string{"A"}

		err := db.ListStringNew(key, values)
		require.NoErrorf(t, err, "Unexpected error send new")

		defer db.Delete(key)

		err = db.ListStringAddRight(key, values)
		require.NoErrorf(t, err, "Unexpected error add right")

		// Test
		rxValue, err := db.ListStringRecvRight(key)
		require.NoErrorf(t, err, "Unexpected error recieve first")
		assert.Equalf(t, values[0], rxValue, "first Value by index 0 is not equal")

		rxValue, err = db.ListStringRecvRight(key)
		require.NoErrorf(t, err, "Unexpected error recieve dublicat")
		assert.Equalf(t, values[0], rxValue, "dublicat Value by index 0 is not equal")

		rxValue, err = db.ListStringRecvRight(key)
		require.Equalf(t, ErrKeyIsNotExists, err, "Error is not equal")

	})

}

// Test ListStringRecvLen
func TestListStringRecvLen(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""

		_, err := db.ListStringRecvLen(key)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Missing key", func(t *testing.T) {

		key := "List:1"

		_, err := db.ListStringRecvLen(key)
		require.Equalf(t, ErrKeyIsNotExists, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		// Prepare
		key := "List:1"
		values := []string{"A", "B", "C"}

		err := db.ListStringNew(key, values)
		require.NoErrorf(t, err, "Unexpected error send new")

		defer db.Delete(key)

		// Test
		rxValue, err := db.ListStringRecvLen(key)
		require.NoErrorf(t, err, "Unexpected error add right")
		assert.Equalf(t, int64(len(values)), rxValue, "Size is not equal")

	})

}

// Test ListStringMoveByNameToNewLeft
func TestListStringMoveByNameToNewLeft(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing srcKey", func(t *testing.T) {

		srcKey := "ListSrc:1"
		destKey := "ListDist:1"
		values := []string{"A", "B", "C"}
		move := values[1]

		// Prepare
		err := db.ListStringNew(srcKey, values)
		require.NoErrorf(t, err, "Unexpected error ListStringNew")
		defer db.Delete(srcKey)

		// Test
		srcKey = ""
		_, err = db.ListStringMoveByNameToNewLeft(srcKey, destKey, move)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Missing destKey", func(t *testing.T) {

		srcKey := "ListSrc:1"
		destKey := "ListDist:1"
		values := []string{"A", "B", "C"}
		move := values[1]

		// Prepare
		err := db.ListStringNew(srcKey, values)
		require.NoErrorf(t, err, "Unexpected error ListStringNew")
		defer db.Delete(srcKey)

		// Test
		destKey = ""
		_, err = db.ListStringMoveByNameToNewLeft(srcKey, destKey, move)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Missing value", func(t *testing.T) {

		srcKey := "ListSrc:1"
		destKey := "ListDist:1"
		values := []string{"A", "B", "C"}
		move := values[1]

		// Prepare
		err := db.ListStringNew(srcKey, values)
		require.NoErrorf(t, err, "Unexpected error ListStringNew")
		defer db.Delete(srcKey)

		// Test
		move = ""
		_, err = db.ListStringMoveByNameToNewLeft(srcKey, destKey, move)
		require.Equalf(t, ErrMissingValue, err, "Error is not equal")
	})

	t.Run("Target list is exists", func(t *testing.T) {

		srcKey := "ListSrc:1"
		destKey := "ListDist:1"
		values := []string{"A", "B", "C"}
		move := values[1]

		// Prepare
		err := db.ListStringNew(srcKey, values)
		require.NoErrorf(t, err, "Unexpected error ListStringNew src")
		defer db.Delete(srcKey)

		err = db.ListStringNew(destKey, values)
		require.NoErrorf(t, err, "Unexpected error ListStringNew dest")
		defer db.Delete(destKey)

		// Test
		_, err = db.ListStringMoveByNameToNewLeft(srcKey, destKey, move)
		require.Equalf(t, ErrKeyIsExists, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		srcKey := "ListSrc:1"
		destKey := "ListDist:1"
		values := []string{"A", "B", "C"}
		move := values[1]

		// Prepare
		err := db.ListStringNew(srcKey, values)
		require.NoErrorf(t, err, "Unexpected error ListStringNew src")
		defer func() {
			db.Delete(srcKey)
			db.Delete(destKey)
		}()

		// Test
		rxVolume, err := db.ListStringMoveByNameToNewLeft(srcKey, destKey, move)
		require.NoErrorf(t, err, "Unexpected error move")
		assert.Equalf(t, int64(1), rxVolume, "Value is not equal")

		rxValue, err := db.ListStringRecvLeft(destKey)
		require.NoErrorf(t, err, "Unexpected error recieve")
		assert.Equalf(t, values[1], rxValue, "rxValue is not equal")

	})
}

// Test ListStringRecvRange
func TestListStringRecvRange(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		indStart := int64(0)
		indStop := int64(1)

		// Test
		_, err = db.ListStringRecvRange(key, indStart, indStop)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Index EQ", func(t *testing.T) {

		key := "List:1"
		indStart := int64(1)
		indStop := int64(1)

		// Test
		_, err = db.ListStringRecvRange(key, indStart, indStop)
		require.Equalf(t, ErrIndex, err, "Error is not equal")
	})

	t.Run("Start GT stop", func(t *testing.T) {

		key := "List:1"
		indStart := int64(2)
		indStop := int64(1)

		// Test
		_, err = db.ListStringRecvRange(key, indStart, indStop)
		require.Equalf(t, ErrIndex, err, "Error is not equal")
	})

	t.Run("Start LT 0", func(t *testing.T) {

		key := "List:1"
		indStart := int64(-2)
		indStop := int64(1)

		// Test
		_, err = db.ListStringRecvRange(key, indStart, indStop)
		require.Equalf(t, ErrIndex, err, "Error is not equal")
	})

	t.Run("top LT 0", func(t *testing.T) {

		key := "List:1"
		indStart := int64(2)
		indStop := int64(-1)

		// Test
		_, err = db.ListStringRecvRange(key, indStart, indStop)
		require.Equalf(t, ErrIndex, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		key := "List:1"
		values := []string{"A", "B", "C"}
		indStart := int64(0)
		indStop := int64(1)

		// Prepare
		err := db.ListStringNew(key, values)
		require.NoErrorf(t, err, "Unexpected error ListStringNew src")
		defer func() {
			db.Delete(key)
		}()

		// Test
		rxData, err := db.ListStringRecvRange(key, indStart, indStop)
		require.NoErrorf(t, err, "Unexpected error recieve range")
		require.Equalf(t, 2, len(rxData), "Value is not equal")
		assert.Equalf(t, values[2], rxData[0], "Value is not equal index 0")
		assert.Equalf(t, values[1], rxData[1], "Value is not equal index 1")
	})
}

// Test ListStringTrimRange
func TestListStringTrimRange(t *testing.T) {

	db, err := New("localhost:6379", "", 0)
	require.NoErrorf(t, err, "Fault DB connect")

	defer func() {
		err := db.Close()
		assert.NoErrorf(t, err, "Unexpected error close connect")
	}()

	t.Run("Missing key", func(t *testing.T) {

		key := ""
		indStart := int64(0)
		indStop := int64(1)

		// Test
		_, err = db.ListStringTrimRange(key, indStart, indStop)
		require.Equalf(t, ErrMissingKey, err, "Error is not equal")
	})

	t.Run("Index EQ", func(t *testing.T) {

		key := "List:1"
		indStart := int64(1)
		indStop := int64(1)

		// Test
		_, err = db.ListStringTrimRange(key, indStart, indStop)
		require.Equalf(t, ErrIndex, err, "Error is not equal")
	})

	t.Run("Start LT 0", func(t *testing.T) {

		key := "List:1"
		indStart := int64(-2)
		indStop := int64(1)

		// Test
		_, err = db.ListStringTrimRange(key, indStart, indStop)
		require.Equalf(t, ErrIndex, err, "Error is not equal")
	})

	t.Run("Correct", func(t *testing.T) {

		key := "List:1"
		values := []string{"A", "B", "C"}
		indStart := int64(1)
		indStop := int64(-1)

		// Prepare
		err := db.ListStringNew(key, values)
		require.NoErrorf(t, err, "Unexpected error ListStringNew src")
		defer func() {
			db.Delete(key)
		}()

		// Test
		rxData, err := db.ListStringTrimRange(key, indStart, indStop)
		require.NoErrorf(t, err, "Unexpected error recieve range")
		assert.Equalf(t, "OK", rxData, "Status is not equal")

		rxSize, err := db.ListStringRecvLen(key)
		require.NoErrorf(t, err, "Unexpected error recieve size")
		assert.Equalf(t, int64(2), rxSize, "Size is not equal")
	})
}
