package gokvstore

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func TestDatabase_Open_EmptyDir(t *testing.T) {
	dir := ""

	db, err := Open(dir, DefaultOptions)
	if err != ErrPathRequired {
		t.Errorf("got %v, expected %v", err, ErrPathRequired)
	}
	if db != nil {
		t.Error("expected db to be nil")
	}
}

func TestDatabase_Open(t *testing.T) {
	dir := "/tmp/test"
	db, err := Open(dir, nil)
	if err != nil {
		t.Error("got error")
	}
	if db == nil {
		t.Error("should have got db instance")
	}
	defer db.Close()
}

func TestDatabasePut_ReadOnly(t *testing.T) {
	dir := "/tmp/test"
	db, err := Open(dir, DefaultOptions)
	if err != nil {
		t.Error("got error")
	}
	err = db.Put([]byte("key"), []byte("value"))
	if err != ErrDataBaseReadOnly {
		t.Errorf("database should be readonly %v", err)
	}
	db.Close()
}

func TestDatabasePut_NilKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	err = db.Put(nil, []byte("value"))
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyRequired, err)
	}
	db.Close()
}

func TestDatabasePut_NilValue(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	err = db.Put([]byte("key"), nil)
	if err != ErrValueRequired {
		t.Errorf("expected %v, got %v", ErrKeyRequired, err)
	}
	db.Close()
}
func TestDatabasePut_EmptyValue(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	err = db.Put([]byte("key"), []byte(""))
	if err != ErrValueRequired {
		t.Errorf("expected %v, got %v", ErrKeyRequired, err)
	}
	db.Close()
}

func TestDatabasePut_EmptyKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	err = db.Put([]byte(""), []byte("value"))
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyRequired, err)
	}
	db.Close()
}

func TestDatabasePut(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		t.Error("Put failed", err)
	}
	db.Close()
}

func TestDatabaseGet_BadKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Get([]byte("BadKey"))
	if err != ErrKeyNotFound {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseGet_NilKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Get(nil)
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseGet_EmptyKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Get([]byte(""))
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}
func TestDatabaseDelete_BadKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Delete([]byte("BadKey"))
	if err != ErrDeleteFailed {
		t.Errorf("expected %v, got %v", ErrDeleteFailed, err)
	}
	db.Close()
}

func TestDatabaseDelete_NilKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Delete(nil)
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseDelete_EmptyKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Delete([]byte(""))
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseDelete(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	err = db.Put([]byte("testkey"), []byte("testvalue"))
	if err != nil {
		t.Error("put failed")
	}
	_, err = db.Delete([]byte("testkey"))
	if err != nil {
		t.Error("delete failed")
	}
	_, err = db.Get([]byte("testkey"))
	if err != ErrKeyNotFound {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseRange_EmptyStartKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Range([]byte(""), []byte("endKey"))
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseRange_NilStartKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Range(nil, []byte("endKey"))
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseRange_EmptyEndKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Range([]byte("startkey"), []byte(""))
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseRange_NilEndKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Range([]byte("startkey"), nil)
	if err != ErrKeyRequired {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabaseRange_StartkeyGTEndKey(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Range([]byte("G"), []byte("A"))
	if err != ErrInvalidRange {
		t.Errorf("expected %v, got %v", ErrInvalidRange, err)
	}
	db.Close()
}

func TestDatabaseRange(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	cur, err := db.Range([]byte("02j5C"), []byte("04ZfI"))
	if err != nil {
		t.Error("range failed")
	}
	if !bytes.Equal(cur.data[1].key, []byte("02j5C")) {
		t.Errorf("expected %s, got %s", []byte("02j5C"), cur.data[1].key)
	}
	if !bytes.Equal(cur.data[len(cur.data)-1].key, []byte("04ZfI")) {
		t.Errorf("expected %s, got %s", []byte("04ZfI"), cur.data[len(cur.data)-1].key)
	}
	db.Close()
}

func TestDatabaseGet(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	err = db.Put([]byte("key"), []byte("value1"))

	if err != nil {
		t.Error("Put failed", err)
	}

	val, err := db.Get([]byte("key"))
	if err != nil {
		t.Error("Get failed", err)
	}
	if !bytes.Equal([]byte("value1"), val) {
		t.Errorf("got %s, expected %s", val, []byte("value1"))
	}

	_, err = db.Get([]byte("BadKey"))
	if err != ErrKeyNotFound {
		t.Errorf("expected %v, got %v", ErrKeyNotFound, err)
	}
	db.Close()
}

func TestDatabase_Closed(t *testing.T) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("got error")
	}
	_, err = db.Close()
	if err != nil {
		t.Error("failed to close database")
	}
	err = db.Put([]byte("key"), []byte("value1"))

	if err != ErrDatabaseClosed {
		t.Errorf("got %v, expected %v", err, ErrDatabaseClosed)
	}

	_, err = db.Get([]byte("key"))
	if err != ErrDatabaseClosed {
		t.Errorf("got %v, expected %v", err, ErrDatabaseClosed)
	}

}
func TestDatabase_GetFromMemDb(t *testing.T) {
	keys := make([][]byte, 0)
	values := make([][]byte, 0)
	dir := "/tmp/test"

	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("failed to open database")
	}

	for j := 0; j < 1000; j++ {
		key := randomBytes(5)
		keys = append(keys, key)
		value := randomBytes(5)
		values = append(values, value)
		err := db.Put(key, value)
		if err != nil {
			t.Error("failed to put")
		}
	}

	for k := 0; k < len(keys); k++ {
		val, err := db.Get(keys[k])
		if err != nil {
			t.Error("Get failed")
		}
		if !bytes.Equal(val, values[k]) {
			t.Errorf("key %s, expected %s, got %s", keys[k], values[k], val)
		}
	}

	db.Close()

}

func TestDatabase_GetFromSStable(t *testing.T) {
	dir := "/tmp/test"
	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		t.Error("failed to open database")
	}

	for j := 0; j < filterSize+10; j++ {
		key := randomBytes(5)
		keys = append(keys, key)
		value := randomBytes(5)
		values = append(values, value)
		err := db.Put(key, value)
		if err != nil {
			t.Error("failed to put")
		}
	}

	for k := 0; k < len(keys); k += 1000 {
		val, err := db.Get(keys[k])
		if err != nil {
			t.Error("Get failed")
		}
		if !bytes.Equal(val, values[k]) {
			t.Errorf("key %s, expected %s, got %s", keys[k], values[k], val)
		}
	}

	db.Close()

}

func BenchmarkDatabase_Put(b *testing.B) {
	dir := "/tmp/test"
	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	db, err := Open(dir, &opts)
	if err != nil {
		b.Error("failed to open database")
	}

	for j := 0; j < b.N; j++ {
		err := db.Put(randomBytes(5), randomBytes(5))
		if err != nil {
			b.Error("failed to put")
		}
	}
	db.Close()

}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func bytesWithCharset(length int, charset string) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return b
}

func randomBytes(length int) []byte {
	return bytesWithCharset(length, charset)
}
