package gokvstore

type Options struct {
	ReadOnly bool

	UseCompression bool

	SyncWrite bool
}

var DefaultOptions = &Options{
	ReadOnly:       true,
	UseCompression: true,
	SyncWrite:      false,
}
