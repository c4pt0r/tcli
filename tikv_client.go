package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/tikv/client-go/v2/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
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
		logutil.BgLogger().Fatal(err.Error())
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
