package gokvstore

import (
	"bytes"
)

type MergingIterator struct {
	iters      []*chunkIterator
	key, value []byte
}

func (mi *MergingIterator) Next() (hasNext bool) {
	if mi.End() {
		return false
	}

	k := mi.Least()
	mi.key = mi.iters[k].Key()
	mi.value = mi.iters[k].Value()
	mi.iters[k].Next()
	return true
}

func (mi *MergingIterator) Key() (key []byte) {
	return mi.key
}

func (mi *MergingIterator) Value() (value []byte) {
	return mi.value
}

func (mi *MergingIterator) Close() (err error) {
	return nil
}

func (mi *MergingIterator) End() bool {
	for _, iter := range mi.iters {
		if !iter.End {
			return false
		}
	}
	return true
}

func (mi *MergingIterator) Least() int {
	k := 0
	for j := k + 1; j < len(mi.iters); j++ {
		if mi.iters[j].End {
			continue
		}
		for mi.iters[k].End {
			k++
		}
		if mi.iters[j].start {
			mi.iters[j].Next()
		}
		if mi.iters[k].start {
			mi.iters[k].Next()
		}
		if bytes.Compare(mi.iters[j].Key(), mi.iters[k].Key()) <= 0 {
			k = j
		}
	}

	return k
}

func NewMergingIterator(iterators []*chunkIterator) *MergingIterator {
	iters := make([]*chunkIterator, 0)
	iters = append(iters, iterators...)
	return &MergingIterator{
		iters: iters,
	}
}
