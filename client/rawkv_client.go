package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/c4pt0r/log"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
)

func newRawKVClient(pdAddr []string) *rawkvClient {
	client, err := rawkv.NewClient(context.TODO(), pdAddr, config.DefaultConfig().Security)
	if err != nil {
		log.F(err)
	}
	return &rawkvClient{
		rawClient: client,
		pdAddr:    pdAddr,
	}
}

type rawkvClient struct {
	rawClient *rawkv.Client
	pdAddr    []string
}

func (c *rawkvClient) Close() {
	if c.rawClient != nil {
		c.rawClient.Close()
	}
}

func (c *rawkvClient) GetClientMode() TiKV_MODE {
	return RAW_CLIENT
}

func (c *rawkvClient) GetClusterID() string {
	return fmt.Sprintf("%d", c.rawClient.ClusterID())
}

func (c *rawkvClient) GetStores() ([]StoreInfo, error) {
	return nil, errors.New("rawkvClient.GetStores() is not implemented")
}

func (c *rawkvClient) GetPDClient() pd.Client {
	log.Fatal("rawkvClient.GetPDClient() is not implemented")
	return nil
}

func (c *rawkvClient) Put(ctx context.Context, kv KV) error {
	return c.rawClient.Put(context.TODO(), kv.K, kv.V)
}

func (c *rawkvClient) PutJson(ctx context.Context, kv KV) error {
	return c.rawClient.Put(context.TODO(), kv.K, kv.V)
} // add

func (c *rawkvClient) BatchPut(ctx context.Context, kv []KV) error {
	return errors.New("rawkvClient.BatchPut() is not implemented")
}

func (c *rawkvClient) Get(ctx context.Context, k Key) (KV, error) {
	v, err := c.rawClient.Get(context.TODO(), k)
	return KV{k, v}, err
}

// add
func (c *rawkvClient) GetJson(ctx context.Context, k Key) (KV, error) {
	v, err := c.rawClient.Get(context.TODO(), k)
	return KV{k, v}, err
}

func (c *rawkvClient) Scan(ctx context.Context, prefix []byte) (KVS, int, error) {
	return nil, 0, errors.New("rawkvClient.Scan() is not implemented")
}

func (c *rawkvClient) Delete(ctx context.Context, k Key) error {
	return errors.New("rawkvClient.Delete() is not implemented")
}

func (c *rawkvClient) BatchDelete(ctx context.Context, kvs []KV) error {
	return errors.New("rawkvClient.BatchDelete() is not implemented")
}

func (c *rawkvClient) DeletePrefix(ctx context.Context, prefix Key, limit int) (Key, int, error) {
	return nil, 0, errors.New("rawkvClient.DeletePrefix() is not implemented")
}
