package gokvstore

import (
	"time"
)

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
