package memfs

import (
	"bytes"
	"fmt"
)

type Record struct {
	Key []byte
	Val []byte
}

func (r Record) Compare(to Comparable) int {

	other := to.(Record)
	return bytes.Compare(r.Key, other.Key)

}

func (r Record) String() string {
	return fmt.Sprintf("%s:%s;", r.Key, r.Val)
}
