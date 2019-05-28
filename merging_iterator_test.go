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
	"testing"
	"bytes"
)

var tmp [50]byte

func TestNewMergingIterator_DifferentKeys(t *testing.T) {
	iters := make([]*chunkIterator, 0)
	data1 := [][]byte{[]byte("4"), []byte("6"), []byte("8")}
	data2 := [][]byte{[]byte("3"), []byte("5"), []byte("7"), []byte("9")}
	data3 := [][]byte{[]byte("1"), []byte("12"), []byte("A"), []byte("B"), []byte("C"), []byte("D")}

	iter1 := NewChunkIterator(createTestData(data1))
	iter2 := NewChunkIterator(createTestData(data2))
	iter3 := NewChunkIterator(createTestData(data3))
	iters = append(iters, iter1)
	iters = append(iters, iter2)
	iters = append(iters, iter3)
	mi := NewMergingIterator(iters)
	data := make([][]byte, 0)
	for mi.Next() {
		data = append(data, mi.Key())
	}
	i := 0
	for j := 1; j < len(data); j++ {
		if bytes.Compare(data[i], data[j]) > 0 {
			t.Errorf("keys should be sorted in ascending order")
		}
		i++
	}
}


func TestNewMergingIterator_SameKeys(t *testing.T) {
	iters := make([]*chunkIterator, 0)
	data1 := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	data2 := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	data3 := [][]byte{[]byte("1"), []byte("2"), []byte("3")}

	iter1 := NewChunkIterator(createTestData(data1))
	iter2 := NewChunkIterator(createTestData(data2))
	iter3 := NewChunkIterator(createTestData(data3))
	iters = append(iters, iter1)
	iters = append(iters, iter2)
	iters = append(iters, iter3)
	mi := NewMergingIterator(iters)
	data := make([][]byte, 0)
	for mi.Next() {
		data = append(data, mi.Key())
	}
	if len(data) != len(data1) {
		t.Errorf("merging iterator should have same number of records")
	}

	for j := 0; j < len(data); j++ {
		if !bytes.Equal(data[j],data1[j]) {
			t.Errorf("merging iterator should have same keys as iterator 1")
		}
	}
}


func TestNewMergingIterator_OverlappingKeys(t *testing.T) {
	iters := make([]*chunkIterator, 0)
	data1 := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	data2 := [][]byte{[]byte("1"), []byte("2"), []byte("4")}
	data3 := [][]byte{[]byte("4"), []byte("5"), []byte("6")}

	iter1 := NewChunkIterator(createTestData(data1))
	iter2 := NewChunkIterator(createTestData(data2))
	iter3 := NewChunkIterator(createTestData(data3))
	iters = append(iters, iter1)
	iters = append(iters, iter2)
	iters = append(iters, iter3)
	mi := NewMergingIterator(iters)
	data := make([][]byte, 0)
	for mi.Next() {
		data = append(data,mi.Key())
	}
	if len(data) != 6 {
		t.Errorf("merginf iterator should not contain duplicate values")
	}
	i := 0
	for j := 1; j < len(data); j++ {
		if bytes.Compare(data[i], data[j]) > 0 {
			t.Errorf("keys should be sorted in ascending order")
		}
		i++
	}
}

func createTestData(data [][]byte) []byte {
	var res = make([]byte, 0)
	for _, b := range data {
		n := binary.PutUvarint(tmp[0:], uint64(len(b)))
		n += binary.PutUvarint(tmp[n:], uint64(len(b)))
		res = append(res, tmp[:n]...)
		res = append(res, b...)
		res = append(res, b...)
	}
	return res
}
