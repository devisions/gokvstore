package gokvstore

import (
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"

	"bytes"
	"github.com/maneeshchaturvedi/gokvstore/memfs"
	"github.com/pkg/errors"
	"github.com/tylertreat/BoomFilters"
	"strings"
)

const (
	//MemdbFileName is the name of the file to which we write C0 if it has not been written
	//to an SSTable
	MemdbFileName = "memfs.gob"
	//CurrentLog is the name of the current write ahead log file.
	CurrentLog    = "writeahead.log"
	//OldLog is what we rename the current write ahead log file to before rotating it.
	OldLog        = "writeahead_old.log"
	//filterSize is the number of keys which we maintain in the Filter and the Memtable. Once this
	//size is exceeded, we write the contents of the Memtable to an SSTable
	filterSize    = 1 << 16
	//deleteMarker is the value we write for a key which has been deleted.
	deleteMarker  = "tombstone/0"
)

var (
	//ErrPathRequired is returned if the path specified while creating the database is nil or empty
	ErrPathRequired     = errors.New("path cannot be nil")
	//ErrKeyRequired is the error returned if the key is nil or empty
	ErrKeyRequired      = errors.New("key cannot be nil")
	//ErrValueRequired is the error returned if the value is nil or empty
	ErrValueRequired    = errors.New("value cannot be nil")
	//ErrDatabaseReadOnly is returned if the database is opened in ReadOnly mode
	// and the client attempts to write to it.
	ErrDatabaseReadOnly = errors.New("database is readonly")
	//ErrDatabaseClosed returned if the database is closed and any other operation
	//except Open is invoked.
	ErrDatabaseClosed   = errors.New("database not open")
	//ErrKeyNotFound is returned if a key we are searching for does not
	//exist in the database
	ErrKeyNotFound      = errors.New("key not found")
	//ErrDeleteFailed is returned if for some reason, deleting a key and associated value fails
	ErrDeleteFailed     = errors.New("failed to delete key")
	//ErrInvalidRange is returned if the start key is larger than the end keys
	ErrInvalidRange     = errors.New("endKey should be greater than startKey")
	//ErrRangeError is returned if the start and end key are not present in the same SSTable
	ErrRangeError       = errors.New("endKey and startkey should be in the same segment")
)
/*
The database struct is what the client uses to work with the database. The client would
call Open specifying the path of the database and the options. Post a call to Open, the client can
use the returned instance of the database to perform operations like Get, Put, Range, Delete etc.


	dir := "path/to/some/dir"
    opts := Options{
    		ReadOnly:       false,
    		UseCompression: true,
    		SyncWrite:      false,
    	}// or use the default options.

	//open the database
	db, err := Open(dir, opts)

	//work with the database
	err = db.Put([]byte("somekey"), []byte("somevalue"))


	val,err := db.Get([]byte("somekey"))

	ok, err = db.Delete([]byte("somekey"))

	cursor,err := db.Range([]byte("startkey"),[]byte("endkey"))
	//iterate over keys and values
	for cursor.Next() {
	    k := cursor.Key()
	    v:=  cursor.Value()
	}
	//close the cursor
	cursor.Close()
	//close the database
	//other operations
	err = db.Close()



 */
type Database struct {
	fs      *FileSystem
	open    bool
	closing bool
	lock    sync.Mutex
	rlock   sync.RWMutex
	options *Options
	memdb   *memfs.Memtable
	filter  *boom.ScalableBloomFilter
	log     *os.File
}

/*
Open is invoked by the client to start working with the database.
In order to do so, the client needs to specify the directory where the database would store its contents.
The client may specify certain options while opening the database.
The call to open provides an instance of the database back to the client,
to enable the client to work with the database to store or retrieve data.
Any C0 component which has not been flushed to disk as a SSTable is loaded
in memory as a Memtable.
 */
func Open(dir string, options *Options) (db *Database, err error) {
	if dir == "" {
		return nil, ErrPathRequired
	}
	if options == nil {
		options = DefaultOptions
	}
	dir = filepath.Clean(dir)
	db = newDB(dir, options)

	_, err = db.fs.OpenDB()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database")
	}
	_, err = os.Stat(path.Join(db.fs.path, MemdbFileName))
	if os.IsExist(err) {
		arr := make([]memfs.Record, 0)
		memdbFile, err := db.fs.OpenFile(MemdbFileName, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load memdb")
		}
		readGob(memdbFile, arr)
		db.memdb = ArrToMemdb(arr)
	}
	db.log, err = db.fs.OpenLogFile(CurrentLog)
	if err != nil {
		return nil, err
	}

	db.open = true
	return db, nil
}

/*
Put saves a key and value pair in the database.
In the event of any issue in saving the key value pair,
an appropriate error is returned back to the client.
 */
func (db *Database) Put(key, value []byte) (err error) {
	if !db.open || db.closing {
		return ErrDatabaseClosed
	}
	if db.options.ReadOnly {
		return ErrDataBaseReadOnly
	}
	if key == nil || len(key) == 0 {
		return ErrKeyRequired
	}
	if value == nil || len(value) == 0 {
		return ErrValueRequired
	}

	db.lock.Lock()
	defer db.lock.Unlock()
	r := memfs.Record{
		Key: key,
		Val: value,
	}
	err = WriteLog(db.log, []byte(r.String()))
	if err != nil {
		return err
	}

	if db.options.SyncWrite {
		err = db.log.Sync()
		if err != nil {
			return err
		}
	}

	if db.memdb.Size() >= filterSize {
		oldMemdb := db.memdb
		db.writeKeysToFilter(oldMemdb)
		err := db.writeSSTable(oldMemdb)
		if err != nil {
			return errors.Wrap(err, "failed to write data to sstable")
		}
		db.memdb = memfs.NewMemtable()
		if err = RotateLog(db); err != nil {
			return errors.Wrap(err, "failed to rotate log file")
		}

		if err := db.fs.DeleteFile(MemdbFileName); err != nil {
			return errors.Wrap(err, "failed to delete memdb file")
		}

	}
	db.memdb.Insert(r)

	return nil
}

func (db *Database) writeKeysToFilter(memdb *memfs.Memtable) {
	sortedRecords := memdb.InOrder()
	for _, c := range sortedRecords {
		r, ok := c.(memfs.Record)
		if ok {
			db.filter.Add(r.Key)
		}
	}

}

func (db *Database) writeSSTable(memdb *memfs.Memtable) (err error) {
	sst, err := db.fs.NewSSTable()
	if err != nil {
		return errors.Wrap(err, "unable to create sstable")
	}
	w := NewWriter(sst, db.options.UseCompression)
	_, err = db.filter.WriteTo(sst.filterfile)
	defer sst.filterfile.Close()
	if err != nil {
		return errors.Wrap(err, "unable to write filter")
	}
	sortedRecords := memdb.InOrder()
	for _, c := range sortedRecords {
		r, ok := c.(memfs.Record)
		if ok {
			w.Set(r.Key, r.Val)
		}
	}
	if err = w.Close(); err != nil {
		return errors.Wrap(err, "failed to write records to sstable")
	}
	db.filter.Reset()
	return nil
}

/*
Get returns the latest value associated with a key or ErrorKeyNotFound if the key does not exist.
Other erroneous cases return appropriate errors.

 */
func (db *Database) Get(key []byte) (value []byte, err error) {
	if !db.open || db.closing {
		return nil, ErrDatabaseClosed
	}
	if key == nil || len(key) == 0 {
		return nil, ErrKeyRequired
	}
	val := db.findInMemDb(key)
	if val != nil {
		if bytes.Equal(val, []byte(deleteMarker)) {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}
	sst, err := db.getSSTWithKey(key)
	if sst == nil {
		return nil, ErrKeyNotFound
	}
	r := NewReader(sst, db.options.UseCompression)
	val, ok := r.Get(key)
	if ok && !bytes.Equal(val, []byte(deleteMarker)) {
		return val, nil
	}

	return nil, ErrKeyNotFound
}

func (db *Database) findInMemDb(key []byte) []byte {

	dummy := memfs.Record{
		Key: key,
		Val: nil,
	}
	c := db.memdb.Get(dummy)
	if c != nil {
		r, ok := c.(memfs.Record)
		if ok {
			return r.Val
		}
	}
	return nil
}

/*
Delete deletes the value associated with a key. The value is assigned a special value tombstone\0.
A deleted key can be resurrected by future writes.
 */
func (db *Database) Delete(key []byte) (ok bool, err error) {
	if !db.open || db.closing {
		return false, ErrDatabaseClosed
	}
	if key == nil || len(key) == 0 {
		return false, ErrKeyRequired
	}

	_, err = db.Get(key)
	if err != nil {
		return false, ErrDeleteFailed
	}
	err = db.Put(key, []byte(deleteMarker))
	if err != nil {
		return false, ErrDeleteFailed
	}
	return true, nil
}

/*
Range allows the client to specify a start and an end key and returns a cursor which can be used to iterate
over the range, start and end key inclusive.
Range queries work only if the key range specified lies within the same SSTable.
 */
func (db *Database) Range(startkey, endKey []byte) (cursor *Cursor, err error) {
	if !db.open || db.closing {
		return nil, ErrDatabaseClosed
	}
	if startkey == nil || len(startkey) == 0 {
		return nil, ErrKeyRequired
	}
	if endKey == nil || len(endKey) == 0 {
		return nil, ErrKeyRequired
	}
	if bytes.Compare(startkey, endKey) > 0 {
		return nil, ErrInvalidRange
	}
	sst1, err := db.getSSTWithKey(startkey)
	if err != nil {
		return nil, err
	}
	sst2, err := db.getSSTWithKey(endKey)
	if err != nil {
		return nil, err
	}

	if strings.Compare(sst1.id, sst2.id) != 0 {
		return nil, ErrRangeError
	}
	r := NewReader(sst1, db.options.UseCompression)
	cursor, err = r.Range(startkey, endKey)
	if err != nil {
		return nil, err
	}
	return cursor, nil

}

func (db *Database) getSSTWithKey(key []byte) (sstable *SSTable, err error) {
	files := GetDataFiles(db.fs.path)
	sort.Sort(ByTime{files, DefaultNameFormat})
	for _, f := range files {
		sst, err := db.fs.OpenSSTable(f)
		if err != nil {
			return nil, errors.Wrap(err, "failed to open sstables for reading")
		}

		db.filter.ReadFrom(sst.filterfile)
		if !db.filter.Test(key) {
			db.filter.Reset()
			sst.filterfile.Close()
			sst.datafile.Close()
			sst.metafile.Close()
			continue
		}
		return sst, nil
	}
	return nil, ErrKeyNotFound
}

/*
Close closes an active database connection and releases any locks acquired on the database.
It also flushes the C0 component to durable storage.
 */
func (db *Database) Close() (ok bool, err error) {
	memdbFile, err := db.fs.OpenFile(MemdbFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return false, errors.Wrap(err, "Failed to write memdb to disk")
	}
	err = writeGob(memdbFile, MemdbToArr(db.memdb))
	if err != nil {
		return false, errors.Wrap(err, "failed to flush memdb")
	}
	db.log.Close()
	ok, err = db.fs.Close()
	if err != nil {
		return ok, errors.Wrap(err, "failed to close database")
	}
	db.closing = true
	db.open = false
	return ok, nil
}
func newDB(path string, options *Options) (db *Database) {
	fs := NewFS(path, options)
	memdb := memfs.NewMemtable()
	db = &Database{
		fs:      fs,
		options: options,
		open:    true,
		closing: false,
		memdb:   memdb,
		filter:  boom.NewDefaultScalableBloomFilter(0.0001),
	}
	return db

}
