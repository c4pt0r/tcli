package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

var (
	propertiesKey = "property"
)

type StoreInfo struct {
	ID      string
	Version string
	Addr    string
	Status  string
}

type PDInfo struct {
	Name       string
	ClientURLs []string
}

func (StoreInfo) TableTitle() []string {
	return []string{"Store ID", "Version", "Address", "Status"}
}

func (s *StoreInfo) Flatten() []string {
	return []string{s.ID, s.Version, s.Addr, s.Status}
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

func initTikvClient() {
	kvClient := NewTikvClient([]string{*pdAddr})
	_globalKvClient.Store(kvClient)
}

func GetTikvClient() *TikvClient {
	return _globalKvClient.Load().(*TikvClient)
}

func NewTikvClient(pdAddr []string) *TikvClient {
	client, err := tikv.NewTxnClient(pdAddr)
	if err != nil {
		//logutil.BgLogger().Fatal(err.Error())
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
			ID:      fmt.Sprintf("%d", store.GetId()),
			Version: store.GetVersion(),
			Addr:    store.GetAddress(),
			Status:  store.GetState().String(),
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

func (c *TikvClient) Scan(ctx context.Context, keyPrefix []byte) (KVS, error) {
	scanOpts := propFromContext(ctx)
	tx, err := c.client.Begin()
	if err != nil {
		return nil, err
	}

	keyOnly := scanOpts.GetBool(ScanOptKeyOnly, false)
	log.Println(keyOnly)
	tx.GetSnapshot().SetKeyOnly(keyOnly)

	limit := scanOpts.GetInt(ScanOptLimit, 100)
	it, err := tx.Iter(keyPrefix, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() && limit > 0 {
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		limit--
		it.Next()
	}
	return ret, nil
}

func (c *TikvClient) BatchPut(ctx context.Context, kv []KV) error {
	return errors.New("not implemented")
}

func (c *TikvClient) Get(ctx context.Context, k Key) (KV, error) {
	return KV{}, errors.New("not implemented")
}

func (c *TikvClient) Delete(ctx context.Context, k Key) error {
	return errors.New("not implemented")
}

func (c *TikvClient) DeleteRange(ctx context.Context, start, end Key) error {
	return errors.New("not implemented")
}
