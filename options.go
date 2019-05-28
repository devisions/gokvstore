package gokvstore


/*
Options specify the options a client can use while connecting to the database.
ReadOnly - is used by a client to connect in a read only mode. No writes are permitted in this mode.

UseCompression - specifies whether to use compression. The default compression uses snappy compression.

SyncWrite - determines whether writes are synchronously written to the write ahead log.
 */
type Options struct {
	ReadOnly bool

	UseCompression bool

	SyncWrite bool
}
/*
The default options if the client does not specify any.
 */
var DefaultOptions = &Options{
	ReadOnly:       true,
	UseCompression: true,
	SyncWrite:      false,
}
