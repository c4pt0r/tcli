package query

type Txn interface {
	Get(key []byte) (value []byte, err error)
	Cursor() (cursor Cursor, err error)
}

type Cursor interface {
	Seek(prefix []byte) error
	Next() (key []byte, value []byte, err error)
}

type KVPair struct {
	Key   []byte
	Value []byte
}

func NewKVP(key []byte, val []byte) KVPair {
	return KVPair{
		Key:   key,
		Value: val,
	}
}

func NewKVPStr(key string, val string) KVPair {
	return KVPair{
		Key:   []byte(key),
		Value: []byte(val),
	}
}
