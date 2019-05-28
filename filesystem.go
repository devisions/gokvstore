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
	"time"

	"github.com/pkg/errors"
	"os"
	"path"
	"syscall"
)

const (
	//lockFileName is the name of the lock file
	lockFileName      = "lock"
	//DefaultNameFormat is the name format of the SSTables.
	DefaultNameFormat = "2006-01-02T15-04-05.000"
	//dataFileExt is the extension of the data files.
	dataFileExt       = ".data"
	//metaFileExt is the extension of the meta files.
	metaFileExt       = ".meta"
	//filterFileExt is the extension of the filter files
	filterFileExt     = ".filter"
)

var (
	//ErrDataBaseReadOnly is returned as the error if the database is opened in ReadOnly mode
	ErrDataBaseReadOnly  error = errors.New("database is readonly")
	//ErrTimeOut is returned if a lock cannot be acquired in a specified time
	ErrTimeout           error = errors.New("operation timed out")
	//ErrFileDoesNotExist is returned if the file does not exist and we are either
	// trying to open the file, delete it or rename it.
	ErrFileDoesNotExist  error = errors.New("file does not exist")
	//ErrFileAlreadyExists is returned if a file already exists. This could happen
	// if we are trying to rename a file and a file with the new name already exists
	ErrFileAlreadyExists error = errors.New("file already exists")
	//current time returns the current time when it was invoked. This is used
	//to creare the identifier for a new SSTable
	currentTime = time.Now
)
/*
Filesystem provided abstractions for interacting with the underlying file system
 */
type FileSystem struct {
	path    string
	options *Options
}

/*
OpenDB accepts the path which is specified by the client. It creates the database directory if it does not exist.
It obtains a lock on the directory. If the client options is ReadOnly, a shared lock is acquired, else an exclusive
lock is acquired.
 */
func (fs *FileSystem) OpenDB() (ok bool, error error) {

	_, err := os.Stat(fs.path)
	if os.IsNotExist(err) {
		err = fs.createDir()
		if err != nil {
			return false, errors.Wrap(err, "can't create directory for new database")
		}

	}
	_, err = os.Stat(path.Join(fs.path, lockFileName))
	if os.IsNotExist(err) {
		if ok, err = fs.obtainLock(fs.options.ReadOnly); err != nil {
			return ok, errors.Wrap(err, "can't obtain lock on database")
		}

	}
	return true, nil
}

func (fs *FileSystem) createDir() (err error) {
	err = os.MkdirAll(fs.path, 0755)
	if err != nil {
		return errors.Wrap(err, "can't make directories for new database")
	}
	_, err = fs.obtainLock(false)
	return err
}

func (fs *FileSystem) obtainLock(readonly bool) (ok bool, err error) {
	lock := path.Join(fs.path, lockFileName)
	lockfile, err := os.Create(lock)
	if err != nil {
		return false, errors.Wrap(err, "can't obtain lock on data directory")
	}
	if !readonly {
		err := flock(int(lockfile.Fd()), true, time.Millisecond*500)
		if err != nil {
			return false, errors.Wrap(err, "failed to lock directory")

		}
	} else {
		err := flock(int(lockfile.Fd()), false, time.Millisecond*500)
		if err != nil {
			return false, errors.Wrap(err, "failed to lock directory")

		}
	}

	return true, nil
}

/*
NewSSTable creates and returns a new SSTable. The id of the SSTable is based on timestamp.
 */
func (fs *FileSystem) NewSSTable() (sst *SSTable, err error) {
	id := sstId()
	df, err := fs.OpenFile(id+dataFileExt, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new sstable")
	}
	mf, err := fs.OpenFile(id+metaFileExt, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new sstable")
	}
	lf, err := fs.OpenFile(id+filterFileExt, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new sstable")
	}

	return &SSTable{
		id:         id,
		datafile:   df,
		metafile:   mf,
		filterfile: lf,
	}, nil
}

/*
OpenSSTable opens the SSTable specified by the id.
 */
func (fs *FileSystem) OpenSSTable(id string) (sst *SSTable, err error) {
	df, err := fs.OpenFile(id+dataFileExt, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open sstable")
	}
	mf, err := fs.OpenFile(id+metaFileExt, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open sstable")
	}
	lf, err := fs.OpenFile(id+filterFileExt, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open sstable")
	}

	return &SSTable{
		id:         id,
		datafile:   df,
		metafile:   mf,
		filterfile: lf,
	}, nil
}

/*
DeleteSSTable deletes the SSTable identified by the id.
 */
func (fs *FileSystem) DeleteSSTable(id string) error {
	dataFile := id + dataFileExt
	metaFile := id + metaFileExt
	filterFile := id + filterFileExt

	err := fs.DeleteFile(dataFile)
	if err != nil {
		return err
	}
	err = fs.DeleteFile(metaFile)
	if err != nil {
		return err
	}
	err = fs.DeleteFile(filterFile)
	if err != nil {
		return err
	}
	return nil
}

/*
Close releases the lock which has been acquired. If the lock is an exclusive lock, we try to release it.
For a shared lock, it is a no-op.
 */
func (fs *FileSystem) Close() (ok bool, error error) {
	lock, err := os.Open(path.Join(fs.path, lockFileName))
	if err != nil {
		return false, errors.Wrap(err, "failed to open lock file")
	}
	if !fs.options.ReadOnly {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
			return false, errors.Wrap(err, "db.Close(): unlock error:")
		}

	}
	return true, nil
}

/*
OpenFile opens a file with the specified name, permissions and mode.
 */
func (fs *FileSystem) OpenFile(name string, flag int, mode os.FileMode) (file *os.File, error error) {
	return os.OpenFile(path.Join(fs.path, name), flag, mode)
}

/*
RenameFile renames the file from the oldName to the newName. If the oldFile does not exist,
ErrFileDoesNotExist is returned. If the newFile exists, ErrFileAlreadyExists is returned.
 */

func (fs *FileSystem) RenameFile(oldName string, newName string) (err error) {
	_, err = os.Stat(path.Join(fs.path, oldName))

	if os.IsNotExist(err) {
		return ErrFileDoesNotExist
	}
	_, err = os.Stat(path.Join(fs.path, newName))

	if os.IsExist(err) {
		return ErrFileAlreadyExists
	}
	return os.Rename(path.Join(fs.path, oldName), path.Join(fs.path, newName))
}
/*
DeleteFile deletes the file with the specified name if exists.
 */
func (fs *FileSystem) DeleteFile(name string) (err error) {
	_, err = os.Stat(path.Join(fs.path, name))

	if os.IsNotExist(err) {
		return nil
	}
	return os.Remove(path.Join(fs.path, name))
}
/*
OpenLogFile opens the write ahead log file. If the file does not exist, it is created.
 */
func (fs *FileSystem) OpenLogFile(name string) (file *os.File, error error) {
	return os.OpenFile(path.Join(fs.path, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
}

func flock(fd int, exclusive bool, timeout time.Duration) error {
	var t time.Time
	for {
		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeout
		}
		flag := syscall.LOCK_SH
		if exclusive {
			flag = syscall.LOCK_EX
		}

		err := syscall.Flock(fd, flag|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func sstId() string {
	t := currentTime()
	return t.Format(DefaultNameFormat)
}
/*
NewFS returns an instance to a new FileSystem. It takes the path where the database needs to be created,
and the client options.
 */
func NewFS(path string, options *Options) *FileSystem {
	return &FileSystem{
		path:    path,
		options: options,
	}
}
