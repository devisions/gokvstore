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
