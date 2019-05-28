/*
 * //  Licensed under the Apache License, Version 2.0 (the "License");
 * //  you may not use this file except in compliance with the
 * //  License. You may obtain a copy of the License at
 * //    http://www.apache.org/licenses/LICENSE-2.0
 * //  Unless required by applicable law or agreed to in writing,
 * //  software distributed under the License is distributed on an "AS
 * //  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * //  express or implied. See the License for the specific language
 * //  governing permissions and limitations under the License.
 */

package gokvstore

import (
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/maneeshchaturvedi/gokvstore/memfs"
	"github.com/pkg/errors"
	"fmt"
)

/*
MemdbToArr converts a memtable into a slice of memfs.Recrods
 */
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

/*
ArrToMemdb converts a slice of Record to a Memtable
 */
func ArrToMemdb(arr []memfs.Record) (memdb *memfs.Memtable) {
	for _, r := range arr {
		memdb.Insert(r)
	}
	return memdb
}

/*
GetDataFiles returns the names of the data files in the specified directory
 */
func GetDataFiles(dir string) []string {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil
	}
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

/*
TimeTrack can be used to instrument code for timing
 */
func TimeTrack(start time.Time, name string) string{
	elapsed := time.Since(start)
	return fmt.Sprintf("%s took %s", name, elapsed)
}

func WriteLog(file *os.File, data []byte) (err error) {
	_, err = file.Write(data)
	if err != nil {
		return err
	}
	return nil
}

/*
RotateLog rotates the write ahead log whenever the contents of a memtable are flushed to disk.
 */
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
