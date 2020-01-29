package sqlds

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/autobatch"
	dssync "github.com/ipfs/go-datastore/sync"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
)

func TestAutoBatching(t *testing.T) {
	opts := &Options{
		Table: "test_datastore",
	}
	store, err := opts.CreatePostgres()
	if err != nil {
		t.Fatal(err)
	}

	batchSize := 16

	d := autobatch.NewAutoBatching(store, batchSize)

	var keys []datastore.Key
	value := []byte("hello world!")
	for i := 0; i < batchSize; i++ {
		key := datastore.NewKey(fmt.Sprintf("key%d", i))
		keys = append(keys, key)
	}
	for _, k := range keys {
		err := d.Put(k, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get works normally.
	for _, k := range keys {
		val, err := d.Get(k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(val, value) {
			t.Fatal("wrong value")
		}
	}

	// Not flushed
	_, err = store.Get(keys[0])
	if err != datastore.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Delete works.
	err = d.Delete(keys[14])
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Get(keys[14])
	if err != datastore.ErrNotFound {
		t.Fatal(err)
	}

	// Still not flushed
	_, err = store.Get(keys[0])
	if err != datastore.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Final put flushes.
	err = d.Put(datastore.NewKey("test16"), value)
	if err != nil {
		t.Fatal(err)
	}

	// should be flushed now, try to get keys from child datastore
	for _, k := range keys[:14] {
		val, err := store.Get(k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(val, value) {
			t.Fatal("wrong value")
		}
	}

	// Never flushed the deleted key.
	_, err = store.Get(keys[14])
	if err != datastore.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Delete doesn't flush
	err = d.Delete(keys[0])
	if err != nil {
		t.Fatal(err)
	}

	val, err := store.Get(keys[0])
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(val, value) {
		t.Fatal("wrong value")
	}
}

func TestProviderManagerDatastore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := &Options{
		Table: "providertest",
	}
	store, err := opts.CreatePostgres()
	if err != nil {
		t.Fatal(err)
	}

	mid := peer.ID("testing")
	p := providers.NewProviderManager(ctx, mid, dssync.MutexWrap(store))
	a := cid.NewCidV0(u.Hash([]byte("test")))
	p.AddProvider(ctx, a.Bytes(), peer.ID("testingprovider"))

	// Not cached
	resp := p.GetProviders(ctx, a.Bytes())
	if len(resp) != 1 {
		t.Fatal("Could not retrieve provider.")
	}

	// Cached
	resp = p.GetProviders(ctx, a.Bytes())
	if len(resp) != 1 {
		t.Fatal("Could not retrieve provider.")
	}

	p.Process().Close()

}
