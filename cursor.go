package gokvstore

type Cursor struct {
	data        []data
	currPointer int
}

type data struct {
	key   []byte
	value []byte
}

func (cursor *Cursor) Next() (hasNext bool) {
	cursor.currPointer++
	return cursor.currPointer < len(cursor.data)
}

func (cursor *Cursor) Key() (key []byte) {
	return cursor.data[cursor.currPointer].key
}

func (cursor *Cursor) Value() (value []byte) {
	return cursor.data[cursor.currPointer].value
}

func (cursor *Cursor) Close() {
	cursor.data = nil
	cursor.currPointer = 0
}

func NewCursor(data []data) *Cursor {

	return &Cursor{
		data:        data,
		currPointer: 0,
	}
}
