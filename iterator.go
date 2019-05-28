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

import "encoding/binary"

/*
chunkIterator is used to iterate over data blocks of an SSTable. It understands the structure of the
SSTable. Each call to Next retrieves the current key and value and removes that from the current data
byte slice.
 */

type chunkIterator struct {
	data       []byte
	key, value []byte
	err        error
	start, End bool
	numKeys    uint64
}

/*
Next is used to traverse over the keys and values in the data slice. Each call to next retrieves the
current key and value and advances to the next key value pair in the data slice.
 */
func (i *chunkIterator) Next() bool {
	if i.End || i.err != nil {
		return false
	}
	if i.start {
		i.start = false
		return true
	}
	if len(i.data) == 0 {
		i.Close()
		return false
	}
	keylen, n0 := binary.Uvarint(i.data)
	vallen, n1 := binary.Uvarint(i.data[n0:])
	n := n0 + n1
	i.key = i.data[n : n+int(keylen)]
	i.value = i.data[n+int(keylen) : n+int(keylen+vallen)]
	i.data = i.data[n+int(keylen+vallen):]
	i.numKeys++
	return true

}

/*
Key returns the current key
 */
func (i *chunkIterator) Key() []byte {
	if i.start {
		return nil
	}
	return i.key[:len(i.key):len(i.key)]
}

/*
Value returns the value associated with the current key.
 */
func (i *chunkIterator) Value() []byte {
	if i.start {
		return nil
	}
	return i.value[:len(i.value):len(i.value)]
}

/*
Close closes the iterator, and resets it.
 */
func (i *chunkIterator) Close() error {
	i.key = nil
	i.value = nil
	i.End = true
	return i.err
}

/*
NewChunkIterator returns a new chunk iterator over the data slice passed to it.
 */
func NewChunkIterator(data []byte) *chunkIterator {
	iter := &chunkIterator{
		data:  data,
		start: true,
		End:   false,
	}
	return iter
}
