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

import "os"

/*
SSTable encapsulates the files associated with an SSTable. Logically,
an SSTable consists of a data file, a meta file and a filter file.
The data file contains contiguous key and value pairs.
Each key and value pair is prepended with the key length and value length,
since these can differ across records.
The meta file contains the index for the SSTable. The index maintains the key
and the offset of the key in the data file.
The filter file contains th bloom filter for the SSTable.
The id identifies an SSTable. The data, meta and filter file have the same name
and differ only in the file extension.

 */
type SSTable struct {
	id         string
	datafile   *os.File
	metafile   *os.File
	filterfile *os.File
}
