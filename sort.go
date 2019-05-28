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
	"time"
)

/*
ByTime implements the Sort interface and is used to sort SSTables by name.
SSTable names are based on a time format, that is, an SSTable created ata latter
point in time has a timestamp name which is larger than previous SSTables.
 */
type ByTime struct {
	files  []string
	format string
}

func (b ByTime) Less(i, j int) bool {
	return b.time(i).After(b.time(j))
}

func (b ByTime) Swap(i, j int) {
	b.files[i], b.files[j] = b.files[j], b.files[i]
}

func (b ByTime) Len() int {
	return len(b.files)
}

func (b ByTime) time(i int) time.Time {
	t, err := time.Parse(b.format, b.files[i])
	if err != nil {
		return time.Time{}
	}
	return t
}
