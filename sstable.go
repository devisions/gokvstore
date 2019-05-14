package gokvstore

import "os"

type SSTable struct {
	id         string
	datafile   *os.File
	metafile   *os.File
	filterfile *os.File
}

func (sst *SSTable) DataFile() *os.File {
	return sst.datafile
}

func (sst *SSTable) MetaFile() *os.File {
	return sst.metafile
}

func (sst *SSTable) FilterFile() *os.File {
	return sst.filterfile
}

func (sst *SSTable) Id() string {
	return sst.id
}
