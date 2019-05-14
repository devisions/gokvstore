package memfs

import "fmt"

type Memtable struct {
	Root *treeNode
	size uint
}

func NewMemtable() *Memtable {
	return &Memtable{}
}

func (memtable *Memtable) Size() uint {
	return memtable.size
}

func (memtable *Memtable) Insert(data Comparable) (err error) {
	if memtable.Root, err = insert(memtable.Root, data); err == nil {
		memtable.size++
	}
	return err
}

func (memtable *Memtable) Delete(data Comparable) (err error) {
	if memtable.Root, err = remove(memtable.Root, data); err == nil {
		memtable.size--
	}
	return err
}

func (memtable *Memtable) Min() (Comparable, error) {
	if memtable.Root == nil {
		return nil, fmt.Errorf("empty tree")
	}
	return min(memtable.Root).data, nil
}

func (memtable *Memtable) Max() (Comparable, error) {
	if memtable.Root == nil {
		return nil, fmt.Errorf("empty tree")
	}
	return maxN(memtable.Root).data, nil
}

func (memtable *Memtable) Height() int {
	return height(memtable.Root)
}

func (memtable *Memtable) Get(data Comparable) Comparable {
	return get(memtable.Root, data)
}

func (memtable *Memtable) InOrder() []Comparable {
	return inOrder(memtable.Root)
}
