package gokvstore

import (
	"encoding/binary"
	"fmt"
	"testing"
)

var tmp [50]byte

func TestNewMergingIterator(t *testing.T) {
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
	for mi.Next() {
		fmt.Printf("key:%s, value:%s\n", mi.Key(), mi.Value())
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
