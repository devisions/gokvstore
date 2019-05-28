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
	"fmt"
	"bytes"
)

/*
MergingIterator takes a slice of chunkIterators and navigates over the data in all the chunk iterators.
Each call to next returns the key and value associated with the smallest key across all the iterators.
If keys are overlapping, it returns the key and value from the iterator containing the most recent update
for that key.
 */
type MergingIterator struct {
	iters                   []*chunkIterator
	key, value              []byte
	numKeysAfterCompaction  uint64
}

/*
Next is used to iterate over the keys and values in all the iterators, returning the smallest
key across all iterators.
 */
func (mi *MergingIterator) Next() (hasNext bool) {
	if mi.end() {
		return false
	}
	mi.numKeysAfterCompaction++
	k := mi.least()
	mi.key = mi.iters[k].Key()
	mi.value = mi.iters[k].Value()
	mi.iters[k].Next()
	return true
}

/*
Key returns the smallest key across all iterators
 */
func (mi *MergingIterator) Key() (key []byte) {
	return mi.key
}

/*
Value returns the value associated with the smallest key.
 */
func (mi *MergingIterator) Value() (value []byte) {
	return mi.value
}

/*
Close closes the iterator. Post a call to Close, the iterator cannot be used.
 */
func (mi *MergingIterator) Close() (err error) {
	for _, iter := range mi.iters {
		err = iter.Close()
		if err != nil {
			fmt.Errorf("Failed to close the iterator %v", err)
		}
	}
	return nil
}

func (mi *MergingIterator) end() bool {
	for _, iter := range mi.iters {
		if !iter.End {
			return false
		}
	}
	return true
}

/*
least returns the iterator index which has the least key across all the iterators.
 */
func (mi *MergingIterator) least() int {
	k := 0
	for j := k + 1; j < len(mi.iters); j++ {
		if mi.iters[k].End {
			k++
			continue
		}
		if mi.iters[j].End {
			continue
		}
		if mi.iters[j].start {
			mi.iters[j].Next()
		}
		if mi.iters[k].start {
			mi.iters[k].Next()
		}
		if mi.iters[k].Key() == nil {
			mi.iters[k].Next()
		}
		if mi.iters[j].Key() == nil {
			mi.iters[j].Next()
		}

		if bytes.Compare(mi.iters[j].Key(), mi.iters[k].Key()) < 0 {
			k = j
		} else if bytes.Equal(mi.iters[j].Key(), mi.iters[k].Key()) {
			mi.iters[j].Next()
		}

	}

	return k
}

/*
NewMergingIterator returns an instance of a MergingIterator containing all the
chunk iterators which have been passed in.
 */
func NewMergingIterator(iterators []*chunkIterator) *MergingIterator {
	iters := make([]*chunkIterator, 0)
	iters = append(iters, iterators...)
	return &MergingIterator{
		iters: iters,
	}
}
