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

package memfs

/*
Memtable implements the C0 component of the LSM tree. It internally uses an AVL tree.
However any tree structure can be swapped instead of an AVL tree, since the only dependence
is on a tree node.
 */
type Memtable struct {
	Root *treeNode
	size uint
}
/*
NewMemtable returns a pointer to a new Memtable
 */
func NewMemtable() *Memtable {
	return &Memtable{}
}
/*
Size returns the size of the Memtable
 */
func (memtable *Memtable) Size() uint {
	return memtable.size
}
/*
Insert inserts data in the Memtable. The data to be inserted must implement
the Comparable interface.
 */
func (memtable *Memtable) Insert(data Comparable) (err error) {
	if memtable.Root, err = insert(memtable.Root, data); err == nil {
		memtable.size++
	}
	return err
}

/*
Get returns the data as a Comparable. This is used when we pass a dummy
record using just the key and return a record containing the key and value.
 */
func (memtable *Memtable) Get(data Comparable) Comparable {
	return get(memtable.Root, data)
}
/*
InOrder traverses the Memtable in order, so the keys are sorted.
Used when we flush the contents of the Memtable to disk.
 */
func (memtable *Memtable) InOrder() []Comparable {
	return inOrder(memtable.Root)
}
