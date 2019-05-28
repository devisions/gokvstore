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
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"bytes"
	"encoding/gob"

	"github.com/pkg/errors"
)

const (
	//blockSize is the default block size is the SSTable
	blockSize = 4096
)

func encodeBlockInfo(dst []byte, b blockInfo) int {
	n := binary.PutUvarint(dst, b.start)
	m := binary.PutUvarint(dst[n:], b.length)
	return n + m
}
/*
index contains the key and the offset of the key in the SSTable.
 */
type index struct {
	Key       []byte
	KeyOffset uint64
}
/*
blockInfo maintains the block start offset for a block in the data file,
and the length of the block.
 */
type blockInfo struct {
	start  uint64
	length uint64
}

/*
Writer is used to create a new SSTable. It writes the contents of the data file and the
meta file to disk.
 */
type Writer struct {
	dataFile             *os.File
	metaFile             *os.File
	filterFile           *os.File
	writer               io.Writer
	bufferedWriter       *bufio.Writer
	metaWriter           io.Writer
	bufferedMetaWriter   *bufio.Writer
	filterWriter         io.Writer
	bufferedFilterWriter *bufio.Writer
	numEntries           int
	offset               uint64
	keyOffset            uint64
	current              blockInfo
	keyIndex             []index
	blocks               []blockInfo
	buf                  []byte
	err                  error
	tmp                  [50]byte
	compress             bool
}
/*
Set writes a key value pair to the data file and writes the offset to the meta file.
 */
func (w *Writer) Set(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	idx := index{
		key,
		w.keyOffset,
	}
	w.keyIndex = append(w.keyIndex, idx)

	w.keyOffset += uint64(len(key) + len(value))
	n := binary.PutUvarint(w.tmp[0:], uint64(len(key)))
	n += binary.PutUvarint(w.tmp[n:], uint64(len(value)))
	w.buf = append(w.buf, w.tmp[:n]...)
	w.buf = append(w.buf, key...)

	w.buf = append(w.buf, value...)
	w.numEntries++
	if len(w.buf) >= blockSize {
		bi, err := w.finishBlock()
		if err != nil {
			w.err = err
			return w.err
		}
		w.current = bi
	}

	return nil
}

func (w *Writer) finishBlock() (blockInfo, error) {
	b := w.buf
	//if w.compress {
	//	b = snappy.Encode(b, w.buf)
	//}

	if _, err := w.writer.Write(b); err != nil {
		return blockInfo{}, err
	}
	bh := blockInfo{w.offset, uint64(len(b))}
	w.offset += uint64(len(b))
	w.blocks = append(w.blocks, bh)
	w.buf = w.buf[:0]
	w.numEntries = 0

	return bh, nil

}
/*
Close closes the writer and flushes the contents of the data file writer
and meta file writer to disk.
 */
func (w *Writer) Close() (err error) {
	defer func() {
		if w.dataFile == nil && w.metaFile == nil {
			return
		}
		err1 := w.dataFile.Close()
		if err == nil {
			err = err1
		}
		w.dataFile = nil
		err1 = w.metaFile.Close()
		if err == nil {
			err = err1
		}
		w.metaFile = nil
	}()
	if w.err != nil {
		return w.err
	}

	if w.numEntries > 0 || len(w.keyIndex) == 0 {
		bh, err := w.finishBlock()
		if err != nil {
			w.err = err
			return w.err
		}
		w.current = bh
	}

	err, n := w.writeIndex()
	if err != nil {
		w.err = err
		return errors.Wrap(err, "failed to write the index")
	}
	err = w.writeBlocks()

	if err != nil {
		w.err = err
		return errors.Wrap(err, "failed to write the block info")
	}
	err = w.writeFooter(n)
	if err != nil {
		w.err = err
		return errors.Wrap(err, "failed to write the footer")
	}

	if w.bufferedWriter != nil {
		if err := w.bufferedWriter.Flush(); err != nil {
			w.err = err
			return err
		}
	}
	if w.bufferedMetaWriter != nil {
		if err := w.bufferedMetaWriter.Flush(); err != nil {
			w.err = err
			return err
		}
	}

	return nil
}

func (w *Writer) writeIndex() (error, int) {

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(w.keyIndex)
	if err != nil {
		w.err = err
		return errors.Wrap(err, "failed to encode the keyIndex"), 0
	}
	n, err := w.metaWriter.Write(buf.Bytes())
	if err != nil {
		w.err = err
		return errors.Wrap(err, "failed to write keyIndex"), 0
	}
	return nil, n
}

func (w *Writer) writeBlocks() error {
	var enc [10]byte
	tmp := make([]byte, 0)
	for _, bi := range w.blocks {
		n := encodeBlockInfo(enc[:], bi)
		tmp = append(tmp, enc[:n]...)
	}
	_, err := w.metaWriter.Write(tmp[0:])
	if err != nil {
		w.err = err
		return errors.Wrap(err, "failed to write block indexes")
	}

	return nil
}

func (w *Writer) writeFooter(n int) error {

	footer := w.tmp[:4]
	binary.PutUvarint(w.tmp[0:], uint64(n))
	if _, err := w.metaWriter.Write(footer); err != nil {
		w.err = err
		return w.err
	}
	return nil
}
/*
NewWriter is used to create a new Writer for a SSTable. It initializes the io.Writer and
the bufio.Writer for the data file and the meta file. If compress is true, the contents of each block
are compressed using snappy compression before being written to disk.
 */
func NewWriter(sst *SSTable, compress bool) *Writer {
	keyIndex := make([]index, 0)
	blocks := make([]blockInfo, 0)
	buf := make([]byte, 0)

	w := &Writer{
		numEntries: 0,
		offset:     0,
		keyOffset:  0,
		keyIndex:   keyIndex,
		blocks:     blocks,
		buf:        buf,
		dataFile:   sst.datafile,
		metaFile:   sst.metafile,
		filterFile: sst.filterfile,
		compress:   compress,
	}
	w.bufferedWriter = bufio.NewWriter(w.dataFile)
	w.bufferedMetaWriter = bufio.NewWriter(w.metaFile)

	w.writer = w.bufferedWriter
	w.metaWriter = w.bufferedMetaWriter
	return w
}
