package query

import (
	"bytes"
	"context"

	"github.com/c4pt0r/kvql"
	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/utils"
	"github.com/magiconair/properties"
)

var (
	_ kvql.Txn    = (*queryTxn)(nil)
	_ kvql.Cursor = (*queryCursor)(nil)
)

type queryTxn struct {
	client client.Client
}

func NewQueryTxn(client client.Client) kvql.Txn {
	return &queryTxn{
		client: client,
	}
}

func (t *queryTxn) Get(key []byte) ([]byte, error) {
	kv, err := t.client.Get(context.TODO(), client.Key(key))
	if err != nil {
		if err.Error() == "not exist" {
			return nil, nil
		}
		return nil, err
	}
	return kv.V, nil
}

func (t *queryTxn) Put(key []byte, value []byte) error {
	return t.client.Put(context.TODO(), client.KV{K: key, V: value})
}

func (t *queryTxn) BatchPut(kvs []kvql.KVPair) error {
	tkvs := make([]client.KV, len(kvs))
	for i, kv := range kvs {
		tkvs[i] = client.KV{K: kv.Key, V: kv.Value}
	}
	return t.client.BatchPut(context.TODO(), tkvs)
}

func (t *queryTxn) Delete(key []byte) error {
	return t.client.Delete(context.TODO(), key)
}

func (t *queryTxn) BatchDelete(keys [][]byte) error {
	tkvs := make([]client.KV, len(keys))
	for i, key := range keys {
		tkvs[i] = client.KV{K: key}
	}
	return t.client.BatchDelete(context.TODO(), tkvs)
}

func (t *queryTxn) Cursor() (kvql.Cursor, error) {
	return &queryCursor{
		txn:     t,
		batch:   nil,
		prefix:  []byte{},
		iterPos: 0,
	}, nil
}

type queryCursor struct {
	txn         *queryTxn
	batch       client.KVS
	batchSize   int
	prefix      []byte
	iterPos     int
	prevLastKey []byte
}

func (c *queryCursor) loadBatch() error {
	scanOpt := properties.NewProperties()
	scanOpt.Set(tcli.ScanOptLimit, "10")
	scanOpt.Set(tcli.ScanOptKeyOnly, "false")
	scanOpt.Set(tcli.ScanOptCountOnly, "false")
	scanOpt.Set(tcli.ScanOptStrictPrefix, "false")
	qctx := utils.ContextWithProp(context.TODO(), scanOpt)
	kvs, n, err := c.txn.client.Scan(qctx, c.prefix)
	if err != nil {
		return err
	}
	c.batch = kvs
	c.batchSize = n
	c.iterPos = 0
	if len(kvs) > 0 {
		// Skip first key
		if c.prevLastKey != nil && bytes.Compare(kvs[0].K, c.prevLastKey) == 0 {
			c.batch = c.batch[1:]
			c.batchSize--
		}
		c.prevLastKey = kvs[len(kvs)-1].K
		c.prefix = c.prevLastKey
	}
	return nil
}

func (c *queryCursor) Next() (key []byte, val []byte, err error) {
	if c.batch == nil || c.iterPos >= c.batchSize {
		err = c.loadBatch()
		if err != nil {
			return nil, nil, err
		}
	}
	if c.batchSize == 0 {
		return nil, nil, nil
	}
	kv := c.batch[c.iterPos]
	c.iterPos++
	key = []byte(kv.K)
	val = []byte(kv.V)
	return key, val, nil
}

func (c *queryCursor) Seek(key []byte) error {
	c.prefix = key
	c.batch = nil
	c.batchSize = 0
	c.iterPos = 0
	c.prevLastKey = nil
	return nil
}
