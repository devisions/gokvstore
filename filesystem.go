package gokvstore

import (
	"os"
	"path"
	"syscall"
	"time"

	"github.com/pkg/errors"
)

const (
	lockFileName      = "lock"
	DefaultNameFormat = "2006-01-02T15-04-05.000"
	dataFileExt       = ".data"
	metaFileExt       = ".meta"
	filterFileExt     = ".filter"
)

var (
	ErrDataBaseReadOnly error = errors.New("database is readonly")
	ErrTimeout          error = errors.New("operation timed out")
	currentTime               = time.Now
)

type FileSystem struct {
	path    string
	options *Options
}

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

func (fs *FileSystem) OpenSSTable(id string) (sst *SSTable, err error) {
	df, err := fs.OpenFile(id+dataFileExt, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new sstable")
	}
	mf, err := fs.OpenFile(id+metaFileExt, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new sstable")
	}
	lf, err := fs.OpenFile(id+filterFileExt, os.O_RDONLY, os.ModePerm)
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

func (fs *FileSystem) OpenFile(name string, flag int, mode os.FileMode) (file *os.File, error error) {
	return os.OpenFile(path.Join(fs.path, name), flag, mode)
}

func (fs *FileSystem) RenameFile(oldName string, newName string) (err error) {
	return os.Rename(path.Join(fs.path, oldName), path.Join(fs.path, newName))
}
func (fs *FileSystem) DeleteFile(name string) (err error) {
	return os.Remove(path.Join(fs.path, name))
}
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

func NewFS(path string, options *Options) *FileSystem {
	return &FileSystem{
		path:    path,
		options: options,
	}
}
