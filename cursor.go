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

/*
Cursor allows a client to iterate over a range of keys. Keys are returned in ascending order,
starting with the lowest to the highest key.
 */
type Cursor struct {
	data        []data
	currPointer int
}

type data struct {
	key   []byte
	value []byte
}

/*
Next is used to sequentially traverse over the records in the cursor.
 */
func (cursor *Cursor) Next() (hasNext bool) {
	cursor.currPointer++
	return cursor.currPointer < len(cursor.data)
}
/*
Key returns the current key which the cursor points to.
 */
func (cursor *Cursor) Key() (key []byte) {
	return cursor.data[cursor.currPointer].key
}
/*
Value returns the value associated with the current key.
 */
func (cursor *Cursor) Value() (value []byte) {
	return cursor.data[cursor.currPointer].value
}
/*
Close closes the Cursor and discards the data associated with the Cursor.
 */
func (cursor *Cursor) Close() {
	cursor.data = nil
	cursor.currPointer = 0
}
/*
NewCursor creates a new cursor, containing the data that is passed in.
 */
func NewCursor(data []data) *Cursor {

	return &Cursor{
		data:        data,
		currPointer: 0,
	}
}
