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
	"encoding/gob"
	"os"

	"github.com/pkg/errors"
)

func writeGob(file *os.File, object interface{}) error {
	encoder := gob.NewEncoder(file)
	err := encoder.Encode(object)
	if err != nil {
		return errors.Wrap(err, "failed to write encoded object to disk")
	}
	file.Close()
	return nil
}

func readGob(file *os.File, object interface{}) error {
	decoder := gob.NewDecoder(file)
	err := decoder.Decode(object)
	if err != nil {
		return errors.Wrap(err, "failed to read encoded object from disk")
	}

	file.Close()
	return nil
}
