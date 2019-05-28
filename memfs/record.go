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

import (
	"bytes"
	"fmt"
)
/*
Record is an implementation of Comparable where comparision compares the keys
of two records.
 */
type Record struct {
	Key []byte
	Val []byte
}
/*
Compare compares the keys of the given Record with this Record
 */
func (r Record) Compare(to Comparable) int {

	other := to.(Record)
	return bytes.Compare(r.Key, other.Key)

}
/*
String represents a print friendy representation of the Record
 */
func (r Record) String() string {
	return fmt.Sprintf("%s:%s;", r.Key, r.Val)
}
