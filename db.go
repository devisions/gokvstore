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
	MemdbFileName = "memfs.gob"
	CurrentLog    = "writeahead.log"
	OldLog        = "writeahead_old.log"
	filterSize    = 1 << 16
	deleteMarker  = "tombstone/0"
)

var (
	ErrPathRequired     = errors.New("path cannot be nil")
	ErrKeyRequired      = errors.New("key cannot be nil")
	ErrValueRequired    = errors.New("value cannot be nil")
	ErrDatabaseReadOnly = errors.New("database is readonly")
	ErrDatabaseClosed   = errors.New("database not open")
	ErrKeyNotFound      = errors.New("key not found")
	ErrDeleteFailed     = errors.New("failed to delete key")
	ErrInvalidRange     = errors.New("endKey should be greater than startKey")
	ErrRangeError       = errors.New("endKey and startkey should be in the same segment")
)

type Database struct {
	fs      *FileSystem
	open    bool
	closing bool
	lock    sync.Mutex
	rlock   sync.RWMutex
	numops  uint64
	options *Options
	memdb   *memfs.Memtable
	filter  *boom.ScalableBloomFilter
	log     *os.File
	logKeys int
}

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
	db.logKeys++
	if err != nil {
		return err
	}

	if db.options.SyncWrite {
		err = db.log.Sync()
		if err != nil {
			return err
		}
	}

	if db.memdb.Size() == filterSize {
		oldMemdb := db.memdb
		db.writeKeysToFilter(oldMemdb)
		err := db.writeSSTable(oldMemdb)
		if err != nil {
			return errors.Wrap(err, "failed to write data to sstable")
		}
		db.memdb = memfs.NewMemtable()
		if err := db.fs.RenameFile(CurrentLog, OldLog); err != nil {
			return errors.Wrap(err, "failed to rename current log")
		}
		if err := db.fs.DeleteFile(OldLog); err != nil {
			return errors.Wrap(err, "failed to delete old log")
		}
		if db.log, err = db.fs.OpenLogFile(CurrentLog); err != nil {
			return errors.Wrap(err, "failed to create new log")
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
	w := NewWriter(sst)
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

func (db *Database) Get(key []byte) (value []byte, err error) {
	if !db.open || db.closing {
		return nil, ErrDatabaseClosed
	}
	if key == nil || len(key) == 0 {
		return nil, ErrKeyRequired
	}
	val := db.findInMemDb(key)
	if val != nil {
		if bytes.Compare(val, []byte(deleteMarker)) == 0 {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}
	sst, err := db.getSSTWithKey(key)
	if sst == nil {
		return nil, ErrKeyNotFound
	}
	r := NewReader(sst)
	val, ok := r.Get(key)
	if ok && bytes.Compare(val, []byte(deleteMarker)) != 0 {
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

	if strings.Compare(sst1.Id(), sst2.Id()) != 0 {
		return nil, ErrRangeError
	}
	r := NewReader(sst1)
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
