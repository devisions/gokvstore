package gokvstore

import "encoding/binary"

type chunkIterator struct {
	data       []byte
	key, value []byte
	err        error
	start, End bool
}

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
	return true

}

func (i *chunkIterator) Key() []byte {
	if i.start {
		return nil
	}
	return i.key[:len(i.key):len(i.key)]
}

func (i *chunkIterator) Value() []byte {
	if i.start {
		return nil
	}
	return i.value[:len(i.value):len(i.value)]
}

func (i *chunkIterator) Close() error {
	i.key = nil
	i.value = nil
	i.End = true
	return i.err
}

func NewChunkIterator(data []byte) *chunkIterator {
	iter := &chunkIterator{
		data:  data,
		start: true,
		End:   false,
	}
	return iter
}
