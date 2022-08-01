package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/c4pt0r/log"
	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/utils"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
)

var MaxRawKVScanLimit = 10240

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

func (c *rawkvClient) BatchPut(ctx context.Context, kvs []KV) error {
	for _, kv := range kvs {
		err := c.rawClient.Put(context.TODO(), kv.K[:], kv.V[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *rawkvClient) Get(ctx context.Context, k Key) (KV, error) {
	v, err := c.rawClient.Get(context.TODO(), k)
	return KV{k, v}, err
}

func (c *rawkvClient) Scan(ctx context.Context, prefix []byte) (KVS, int, error) {
	scanOpts := utils.PropFromContext(ctx)

	strictPrefix := scanOpts.GetBool(tcli.ScanOptStrictPrefix, false)
	countOnly := scanOpts.GetBool(tcli.ScanOptCountOnly, false)
	keyOnly := scanOpts.GetBool(tcli.ScanOptKeyOnly, false)
	// count only mode will ignore this
	limit := scanOpts.GetInt(tcli.ScanOptLimit, 100)
	if countOnly {
		limit = MaxRawKVScanLimit
	}

	keys, values, err := c.rawClient.Scan(ctx, prefix, []byte{}, limit)
	if err != nil {
		return nil, 0, err
	}

	var ret []KV
	var lastKey KV
	count := 0
	for i := 0; i < len(keys); i++ {
		if strictPrefix && !bytes.HasPrefix(keys[i], prefix) {
			break
		}
		if !countOnly {
			if keyOnly {
				ret = append(ret, KV{K: keys[i], V: nil})
			} else {
				ret = append(ret, KV{K: keys[i], V: values[i]})
			}
		}
		count++
		lastKey.K = keys[i]
	}
	if countOnly {
		ret = append(ret, KV{K: []byte("Count"), V: []byte(fmt.Sprintf("%d", count))})
		ret = append(ret, KV{K: []byte("Last Key"), V: []byte(lastKey.K)})
	}
	return ret, count, nil
}

func (c *rawkvClient) Delete(ctx context.Context, k Key) error {
	err := c.rawClient.Delete(context.TODO(), []byte(k))
	return err
}

func (c *rawkvClient) BatchDelete(ctx context.Context, kvs []KV) error {
	keys := [][]byte{}
	for _, kv := range kvs {
		keys = append(keys, []byte(kv.K))
	}
	return c.rawClient.BatchDelete(context.TODO(), keys)
}

func (c *rawkvClient) DeletePrefix(ctx context.Context, prefix Key, limit int) (Key, int, error) {
	endKey := []byte(prefix)
	endKey[len(endKey)] += 0x1
	keys, _, err := c.rawClient.Scan(ctx, prefix, endKey, MaxRawKVScanLimit)
	if err != nil {
		return nil, 0, err
	}
	lastKey := Key(keys[len(keys)-1])
	return lastKey, len(keys), c.rawClient.BatchDelete(context.TODO(), keys)
}
