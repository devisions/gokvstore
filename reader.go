package gokvstore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sort"
	"syscall"

	"github.com/pkg/errors"
)

func decodeBlockInfo(src []byte) (blockInfo, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return blockInfo{}, 0
	}
	return blockInfo{offset, length}, n + m
}

type block []byte

type Reader struct {
	datafile   *os.File
	metafile   *os.File
	filterfile *os.File
	err        error
	keyIndex   []index
	blocks     []blockInfo
}

func (r *Reader) seek(key []byte, i int) *chunkIterator {
	var offset uint64
	var bi blockInfo
	if i < len(r.keyIndex) && i > 0 {
		offset = r.keyIndex[i].KeyOffset + uint64(2*i)
		for _, block := range r.blocks {
			if block.start < offset && offset <= (block.start+block.length) {
				bi = block
				break
			}
		}
	} else if i == len(r.keyIndex) {
		bi = r.blocks[len(r.blocks)-1]
		offset = bi.start + bi.length
	}
	data, err := r.readBlock(bi)
	if err != nil {
		r.err = err
	}
	iter := NewChunkIterator(data[0 : offset-bi.start])
	for iter.Next() && bytes.Compare(iter.Key(), key) < 0 {
	}

	if iter.err != nil {
		r.err = iter.err
	}
	iter.start = !iter.End
	return iter
}

func (r *Reader) Range(startkey, endkey []byte) (cursor *Cursor, err error) {
	d := make([]data, 0)
	idx1, found := r.hasKey(startkey)
	if !found {
		return nil, ErrKeyNotFound
	}
	keyOffset1 := r.keyIndex[idx1-1].KeyOffset + uint64(2*(idx1-1))
	idx2, found := r.hasKey(endkey)
	if !found {
		return nil, ErrKeyNotFound
	}
	keyOffset2 := r.keyIndex[idx2].KeyOffset + uint64(2*idx2)
	bi := blockInfo{
		start:  uint64(keyOffset1),
		length: keyOffset2 - keyOffset1,
	}
	blkData, err := r.readBlock(bi)
	if err != nil {
		r.err = err
	}
	iter := NewChunkIterator(blkData)
	for iter.Next() {
		kvpair := data{iter.Key(), iter.Value()}
		d = append(d, kvpair)
	}
	return NewCursor(d), nil
}

func (r *Reader) hasKey(key []byte) (int, bool) {
	i := sort.Search(len(r.keyIndex), func(i int) bool {
		return bytes.Compare(r.keyIndex[i].Key, key) > 0
	})
	if i > len(r.keyIndex) {
		return -1, false
	}
	if bytes.Equal(r.keyIndex[i-1].Key, key) {
		return i, true
	}
	return -1, false
}
func (r *Reader) Get(key []byte) ([]byte, bool) {
	if r.err != nil {
		return nil, false
	}
	var value []byte
	idx, found := r.hasKey(key)
	if !found {
		return nil, false
	}
	iter := r.seek(key, idx)
	for iter.Next() && iter.err == nil {
		k := iter.Key()
		if bytes.Equal(k, key) {
			value = iter.Value()
			break
		}
	}
	if value == nil && iter.err != nil {
		return nil, false
	}

	return value, true
}

func (r *Reader) readBlock(bi blockInfo) (block, error) {
	data, err := syscall.Mmap(int(r.datafile.Fd()), int64(bi.start), int(bi.length), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		errors.Wrap(err, "failed to mmap the datafile for reading")
		r.err = err
	}

	return data, nil
}

func (r *Reader) readIndex(offset uint64) ([]index, error) {
	data, err := syscall.Mmap(int(r.metafile.Fd()), 0, int(offset), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		errors.Wrap(err, "failed to mmap the metafile for reading")
		r.err = err
	}
	var buf bytes.Buffer
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	decoder := gob.NewDecoder(&buf)
	var keyIndex []index

	if err := decoder.Decode(&keyIndex); err != nil {
		return nil, err

	}
	return keyIndex, nil
}

func (r *Reader) readBlockInfo(offset uint64, bufLen int64) ([]blockInfo, error) {
	b := make([]byte, bufLen)

	if _, err := r.metafile.ReadAt(b, int64(offset)); err != nil {
		return nil, err
	}
	blocks := make([]blockInfo, 0)
	i, _ := 0, 1
	for  {
		bi, n := decodeBlockInfo(b[i:])
		if bi.length == 0 {
			break
		}
		i += n
		blocks = append(blocks, bi)
	}
	return blocks, nil

}

func NewReader(sst *SSTable) *Reader {
	keyIndex := make([]index, 0)
	blocks := make([]blockInfo, 0)

	r := &Reader{
		keyIndex:   keyIndex,
		blocks:     blocks,
		datafile:   sst.DataFile(),
		metafile:   sst.MetaFile(),
		filterfile: sst.FilterFile(),
	}

	if r.datafile == nil {
		r.err = errors.New("nil datafile")
		panic(r.err)
	}
	stat, err := r.datafile.Stat()
	if err != nil {
		r.err = fmt.Errorf("invalid sstable, could not stat datafile: %v", err)
		panic(r.err)
	}
	if r.filterfile == nil {
		r.err = errors.New("nil filterfile")
		panic(r.err)
	}
	stat, err = r.filterfile.Stat()
	if err != nil {
		r.err = fmt.Errorf("invalid sstable, could not stat filterfile: %v", err)
		panic(r.err)
	}
	if r.metafile == nil {
		r.err = errors.New("nil metafile")
		panic(r.err)
	}
	stat, err = r.metafile.Stat()
	if err != nil {
		r.err = fmt.Errorf("invalid sstable, could not stat metafile: %v", err)
		panic(r.err)
	}
	var footer [4]byte
	if stat.Size() < int64(len(footer)) {
		r.err = errors.New("invalid table, metafile size is too small")
		return r
	}
	n, err := r.metafile.ReadAt(footer[:], stat.Size()-int64(len(footer)))
	if err != nil && err != io.EOF {
		r.err = fmt.Errorf("invalid table, could not read footer: %v", err)
		return r
	}

	offset, n := binary.Uvarint(footer[0:])

	if n == 0 {
		r.err = errors.New("invalid table, bad footer")
		return r
	}
	bufLength := stat.Size() - int64(offset) - int64(n)
	r.blocks, r.err = r.readBlockInfo(offset, bufLength)
	r.keyIndex, r.err = r.readIndex(offset)
	return r
}
