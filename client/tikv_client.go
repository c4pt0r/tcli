package client

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"tcli"
	"tcli/utils"

	"github.com/c4pt0r/log"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

var (
	propertiesKey = "property"
)

type StoreInfo struct {
	ID            string
	Version       string
	Addr          string
	State         string
	StatusAddress string
}

type PDInfo struct {
	Name       string
	ClientURLs []string
}

func (StoreInfo) TableTitle() []string {
	return []string{"Store ID", "Version", "Address", "State", "Status Address"}
}

func (s *StoreInfo) Flatten() []string {
	return []string{s.ID, s.Version, s.Addr, s.State, s.StatusAddress}
}

type TikvClient struct {
	client *tikv.KVStore
	pdAddr []string
}

// Make sure TikvClient implements Client interface
var _ Client = (*TikvClient)(nil)

// Global client instance, safe to use concurrently
var (
	_globalKvClient atomic.Value
)

func InitTikvClient(pdAddrs []string) {
	kvClient := NewTikvClient(pdAddrs)
	_globalKvClient.Store(kvClient)
}

func GetTikvClient() *TikvClient {
	return _globalKvClient.Load().(*TikvClient)
}

func NewTikvClient(pdAddr []string) *TikvClient {
	client, err := tikv.NewTxnClient(pdAddr)
	if err != nil {
		log.F(err)
	}
	return &TikvClient{
		client: client,
		pdAddr: pdAddr,
	}
}

func (c *TikvClient) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

func (c *TikvClient) GetClusterID() string {
	return fmt.Sprintf("%d", c.client.GetPDClient().GetClusterID(context.TODO()))
}

func (c *TikvClient) GetStores() ([]StoreInfo, error) {
	var ret []StoreInfo
	stores, err := c.client.GetPDClient().GetAllStores(context.TODO())
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

func (c *TikvClient) GetPDClient() pd.Client {
	return c.client.GetPDClient()
}

func (c *TikvClient) Put(ctx context.Context, kv KV) error {
	tx, err := c.client.Begin()
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

func (c *TikvClient) Scan(ctx context.Context, startKey []byte) (KVS, error) {
	scanOpts := utils.PropFromContext(ctx)
	tx, err := c.client.Begin()
	if err != nil {
		return nil, err
	}

	strictPrefix := scanOpts.GetBool(tcli.ScanOptStrictPrefix, false)
	countOnly := scanOpts.GetBool(tcli.ScanOptCountOnly, false)
	keyOnly := scanOpts.GetBool(tcli.ScanOptKeyOnly, false)
	if keyOnly || countOnly {
		tx.GetSnapshot().SetKeyOnly(keyOnly)
	}

	limit := scanOpts.GetInt(tcli.ScanOptLimit, 100)
	it, err := tx.Iter(startKey, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var ret []KV
	var lastKey KV
	count := 0
	for it.Valid() && limit > 0 {
		if strictPrefix && !bytes.HasPrefix(it.Key(), startKey) {
			break
		}
		if !countOnly {
			ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		} else {
			count++
		}
		limit--
		lastKey.K = it.Key()[:]
		it.Next()
	}
	if countOnly {
		ret = append(ret, KV{K: []byte("Count"), V: []byte(fmt.Sprintf("%d", count))})
		ret = append(ret, KV{K: []byte("Last Key"), V: []byte(lastKey.K)})
	}
	return ret, nil
}

func (c *TikvClient) BatchPut(ctx context.Context, kvs []KV) error {
	tx, err := c.client.Begin()
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

func (c *TikvClient) Get(ctx context.Context, k Key) (KV, error) {
	tx, err := c.client.Begin()
	if err != nil {
		return KV{}, err
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil {
		return KV{}, err
	}
	return KV{K: k, V: v}, nil
}

func (c *TikvClient) Delete(ctx context.Context, k Key) error {
	tx, err := c.client.Begin()
	if err != nil {
		return err
	}
	tx.Delete(k)
	return tx.Commit(context.Background())

}

// return lastKey, delete count, error
func (c *TikvClient) DeletePrefix(ctx context.Context, prefix Key, limit int) (Key, int, error) {
	tx, err := c.client.Begin()
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
		log.D(it.Key(), prefix, bytes.HasPrefix(it.Key(), prefix))
		lastKey.K = it.Key()[:]
		batch = append(batch, KV{K: it.Key()[:]})
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
		log.D(batch)
		if err := c.BatchDelete(ctx, batch); err != nil {
			return nil, count, err
		}
		count += len(batch)
	}
	return lastKey.K, count, nil
}

func (c *TikvClient) BatchDelete(ctx context.Context, kvs []KV) error {
	tx, err := c.client.Begin()
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
