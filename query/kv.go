package query

type Txn interface {
	Get(key []byte) (value []byte, err error)
	Cursor() (cursor Cursor, err error)
}

type Cursor interface {
	Seek(prefix []byte) error
	Next() (key []byte, value []byte, err error)
}
