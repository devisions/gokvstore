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
	"encoding/binary"
	"os"
	"bytes"
	"sort"
	"syscall"
	"github.com/pkg/errors"
	"encoding/gob"
	"fmt"
	"io"
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

/*
Reader is used to read the SSTable and retrive the values associated with a key. A reader understands the internal
structure of a SSTable and loads the data and the meta files. It maintains the key index, containing the key and
its offset in-memory. It determines which block the key resides in and loads that block using the key offset.
For range queries, it uses the offset of the start and end keys to load the data.
 */
type Reader struct {
	datafile   *os.File
	metafile   *os.File
	filterfile *os.File
	err        error
	keyIndex   []index
	blocks     []blockInfo
	compress   bool
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

/*
Range is used for range queries. It loads the data from the SSTable starting at the start key offset,
till the end key offset. It returns a Cursor which is used to traverse over the range. The start and end keys
must reside in the same SSTable.
 */
func (r *Reader) Range(startkey, endkey []byte) (cursor *Cursor, err error) {
	if r.err != nil {
		return nil, r.err
	}

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

/*
Get returns the value associated with a particular key. It finds the block within the SSTable in which
the key resides and seeks to the offset of the key to return the value.
 */
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

	//b := make([]byte, bi.length)
	data, err := syscall.Mmap(int(r.datafile.Fd()), int64(bi.start), int(bi.length), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, errors.Wrap(err, "failed to mmap the datafile for reading")
	}

	//if r.compress {
	//	b, err = snappy.Decode(nil, data)
	//	if err != nil {
	//		return nil, errors.Wrap(err, "failed to uncompress data")
	//
	//	}
	//}
	//fmt.Printf("decompressed data %s\n,",string(b))
	return data, nil
}

func (r *Reader) readIndex(offset uint64) ([]index, error) {
	data, err := syscall.Mmap(int(r.metafile.Fd()), 0, int(offset), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, errors.Wrap(err, "failed to mmap the metafile for reading")
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
	for {
		bi, n := decodeBlockInfo(b[i:])
		if bi.length == 0 {
			break
		}
		i += n
		blocks = append(blocks, bi)
	}
	return blocks, nil

}

/*
NewReader returns a Reader for an SSTable. If compress is true, it uncompresses the data post reading it.
The returned reader is initialized and ready to use i.e, the meta file containing the index and the block
information for all the blocks in the SSTable, is loaded in memory during this call. Calls to Get is where the data file is read based on the offset of
the key in the SSTable.
 */
func NewReader(sst *SSTable, compress bool) *Reader {
	keyIndex := make([]index, 0)
	blocks := make([]blockInfo, 0)

	r := &Reader{
		keyIndex:   keyIndex,
		blocks:     blocks,
		datafile:   sst.datafile,
		metafile:   sst.metafile,
		filterfile: sst.filterfile,
		compress:   compress,
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
