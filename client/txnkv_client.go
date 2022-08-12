package client

import (
	"bytes"
	"context"
	"fmt"
	"tcli"
	"tcli/utils"

	"github.com/c4pt0r/log"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

func newTxnKVClient(pdAddr []string) *txnkvClient {
	client, err := tikv.NewTxnClient(pdAddr)
	if err != nil {
		log.F(err)
	}
	return &txnkvClient{
		txnClient: client,
		pdAddr:    pdAddr,
	}
}

type txnkvClient struct {
	txnClient *tikv.KVStore
	pdAddr    []string
}

func (c *txnkvClient) Close() {
	if c.txnClient != nil {
		c.txnClient.Close()
	}
}

func (c *txnkvClient) GetClientMode() TiKV_MODE {
	return TXN_CLIENT
}

func (c *txnkvClient) GetClusterID() string {
	return fmt.Sprintf("%d", c.txnClient.GetPDClient().GetClusterID(context.TODO()))
}

func (c *txnkvClient) GetStores() ([]StoreInfo, error) {
	var ret []StoreInfo
	stores, err := c.txnClient.GetPDClient().GetAllStores(context.TODO())
	if err != nil {
		return nil, err
	}
	for _, store := range stores {
		ret = append(ret, StoreInfo{
			ID:            fmt.Sprintf("%d", store.GetId()),
			Version:       store.GetVersion(),
			Addr:          store.GetAddress(),
			State:         store.GetState().String(),
			StatusAddress: store.GetStatusAddress(),
		})
	}
	return ret, nil
}

func (c *txnkvClient) GetPDClient() pd.Client {
	return c.txnClient.GetPDClient()
}

func (c *txnkvClient) Put(ctx context.Context, kv KV) error {
	tx, err := c.txnClient.Begin()
	if err != nil {
		return err
	}

	tx.Set(kv.K, kv.V)

	err = tx.Commit(context.TODO())
	if err != nil {
		err = tx.Rollback()
		if err != nil {
			return err
		}
	}
	return nil
}

// add
func (c *txnkvClient) PutJson(ctx context.Context, kv KV) error {
	tx, err := c.txnClient.Begin()
	if err != nil {
		return err
	}

	tx.Set(kv.K, kv.V)

	err = tx.Commit(context.TODO())
	if err != nil {
		err = tx.Rollback()
		if err != nil {
			return err
		}
	}
	return nil
}


func (c *txnkvClient) Scan(ctx context.Context, startKey []byte) (KVS, int, error) {
	scanOpts := utils.PropFromContext(ctx)
	tx, err := c.txnClient.Begin()
	if err != nil {
		return nil, 0, err
	}

	strictPrefix := scanOpts.GetBool(tcli.ScanOptStrictPrefix, false)
	countOnly := scanOpts.GetBool(tcli.ScanOptCountOnly, false)
	keyOnly := scanOpts.GetBool(tcli.ScanOptKeyOnly, false)
	if keyOnly || countOnly {
		tx.GetSnapshot().SetKeyOnly(keyOnly)
	}
	// count only mode will ignore this
	limit := scanOpts.GetInt(tcli.ScanOptLimit, 100)
	it, err := tx.Iter(startKey, nil)
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()

	var ret []KV
	var lastKey KV
	count := 0
	for it.Valid() {
		if !countOnly && limit == 0 {
			break
		}
		if strictPrefix && !bytes.HasPrefix(it.Key(), startKey) {
			break
		}
		// count only will not use limit
		if !countOnly {
			ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
			limit--
		}
		count++
		lastKey.K = it.Key()[:]
		it.Next()
	}
	if countOnly {
		ret = append(ret, KV{K: []byte("Count"), V: []byte(fmt.Sprintf("%d", count))})
		ret = append(ret, KV{K: []byte("Last Key"), V: []byte(lastKey.K)})
	}
	return ret, count, nil
}

func (c *txnkvClient) BatchPut(ctx context.Context, kvs []KV) error {
	tx, err := c.txnClient.Begin()
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		err := tx.Set(kv.K[:], kv.V[:])
		if err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}

func (c *txnkvClient) Get(ctx context.Context, k Key) (KV, error) {
	tx, err := c.txnClient.Begin()
	if err != nil {
		return KV{}, err
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil {
		return KV{}, err
	}
	return KV{K: k, V: v}, nil
}

//add
func (c *txnkvClient) GetJson(ctx context.Context, k Key) (KV, error) {
	tx, err := c.txnClient.Begin()
	if err != nil {
		return KV{}, err
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil {
		return KV{}, err
	}
	return KV{K: k, V: v}, nil
}

func (c *txnkvClient) Delete(ctx context.Context, k Key) error {
	tx, err := c.txnClient.Begin()
	if err != nil {
		return err
	}
	tx.Delete(k)
	return tx.Commit(context.Background())
}

// return lastKey, delete count, error
func (c *txnkvClient) DeletePrefix(ctx context.Context, prefix Key, limit int) (Key, int, error) {
	tx, err := c.txnClient.Begin()
	if err != nil {
		return nil, 0, err
	}

	tx.GetSnapshot().SetKeyOnly(true)

	it, err := tx.Iter(prefix, nil)
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()

	var lastKey KV
	count := 0

	var batch []KV

	for it.Valid() && limit > 0 {
		if !bytes.HasPrefix(it.Key(), prefix) {
			break
		}
		lastKey.K = it.Key()[:]
		batch = append(batch, KV{K: it.Key()[:]})
		// TODO batch size shoule not be fixed
		if len(batch) == 1000 {
			// do delete
			if err := c.BatchDelete(ctx, batch); err != nil {
				return lastKey.K, count, err
			}
			count += len(batch)
			// reset batch
			batch = nil
		}
		limit--
		it.Next()
	}
	if len(batch) > 0 {
		if err := c.BatchDelete(ctx, batch); err != nil {
			return nil, count, err
		}
		count += len(batch)
	}
	return lastKey.K, count, nil
}

func (c *txnkvClient) BatchDelete(ctx context.Context, kvs []KV) error {
	tx, err := c.txnClient.Begin()
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		err := tx.Delete(kv.K)
		if err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}
