package gokvstore

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/maneeshchaturvedi/gokvstore/memfs"
	"github.com/pkg/errors"
)

func MemdbToArr(memdb *memfs.Memtable) (arr []memfs.Record) {
	arr = make([]memfs.Record, 0)
	sorted := memdb.InOrder()
	for _, c := range sorted {
		r, ok := c.(memfs.Record)
		if ok {
			arr = append(arr, r)
		}
	}
	return arr
}

func ArrToMemdb(arr []memfs.Record) (memdb *memfs.Memtable) {
	for _, r := range arr {
		memdb.Insert(r)
	}
	return memdb
}

func GetDataFiles(dir string) []string {
	var files []string
	filepath.Walk(dir, func(dir string, f os.FileInfo, _ error) error {
		if !f.IsDir() {
			r, err := regexp.MatchString(dataFileExt, f.Name())
			if err == nil && r {
				name := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
				files = append(files, name)
			}
		}
		return nil
	})
	return files
}

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func WriteLog(file *os.File, data []byte) (err error) {
	_, err = file.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func RotateLog(db *Database) (err error) {
	if err := db.fs.RenameFile(CurrentLog, OldLog); err != nil {
		return errors.Wrap(err, "failed to rename current log")
	}
	if err := db.fs.DeleteFile(OldLog); err != nil {
		return errors.Wrap(err, "failed to delete old log")
	}
	if db.log, err = db.fs.OpenLogFile(CurrentLog); err != nil {
		return errors.Wrap(err, "failed to create new log")
	}
	return nil
}

