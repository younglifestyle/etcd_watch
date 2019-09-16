package main

import (
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

// EvtType event type
type EvtType int

// EvtKey event key string
type EvtKey string

// Evt event
type Evt struct {
	//Src   EvtSrc
	Type  EvtType
	Key   string
	Value interface{}
}

// BasicAuth basic auth
type BasicAuth struct {
	userName string
	password string
}

const (
	// EventTypeNew event type new
	EventTypeNew = EvtType(0)
	// EventTypeUpdate event type update
	EventTypeUpdate = EvtType(1)
	// EventTypeDelete event type delete
	EventTypeDelete = EvtType(2)
)

const (
	// DefaultTimeout default timeout
	DefaultTimeout = time.Second * 3
)

// EtcdStore etcd store impl
type EtcdStore struct {
	//sync.RWMutex

	prefix  string
	keysMap map[string]struct{}

	evtCh              chan *Evt
	watchMethodMapping map[EvtKey]func(EvtType, *mvccpb.KeyValue) *Evt

	rawClient *clientv3.Client
}

// 设计思想 :
// 1、判断nil，非空无数据等操作，由调用者自己控制，均不设计此类判断在库中
// 2、公共函数需要添加注释

// NewEtcdStore create a etcd store
// /127.0.0.1/file.json  prefix : /127.0.0.1/ , key : file.json
func NewEtcdStore(etcdAddrs []string,
	prefix string,
	basicAuth BasicAuth,
	watchHandle map[EvtKey]func(EvtType, *mvccpb.KeyValue) *Evt) (*EtcdStore, error) {

	store := &EtcdStore{
		prefix:  prefix,
		keysMap: make(map[string]struct{}),
		watchMethodMapping: func() map[EvtKey]func(EvtType, *mvccpb.KeyValue) *Evt {
			if watchHandle == nil {
				return make(map[EvtKey]func(EvtType, *mvccpb.KeyValue) *Evt)
			}
			return watchHandle
		}(),
	}

	config := &clientv3.Config{
		Endpoints:   etcdAddrs,
		DialTimeout: DefaultTimeout,
	}
	if basicAuth.userName != "" {
		config.Username = basicAuth.userName
	}
	if basicAuth.password != "" {
		config.Password = basicAuth.password
	}

	if len(store.watchMethodMapping) == 0 {
		// no key input, default use prefix as key
		store.keysMap[prefix] = struct{}{}
		store.watchMethodMapping[EvtKey(prefix)] = defaultDataHandle
	} else {
		for key, watchMeth := range store.watchMethodMapping {
			store.keysMap[prefix] = struct{}{}
			if watchMeth == nil {
				store.watchMethodMapping[key] = defaultDataHandle
			}
		}
	}

	cli, err := clientv3.New(*config)
	if err != nil {
		return nil, err
	}

	store.rawClient = cli

	return store, nil
}

func defaultDataHandle(evtType EvtType, kv *mvccpb.KeyValue) *Evt {

	return &Evt{
		Type:  evtType,
		Key:   string(kv.Key),
		Value: kv.Value,
	}
}

// Watch watch event from etcd
func (e *EtcdStore) Watch(evtCh chan *Evt) error {
	e.evtCh = evtCh

	e.doWatch()

	return nil
}

func (e *EtcdStore) doWatch() {
	watcher := clientv3.NewWatcher(e.rawClient)
	defer watcher.Close()

	ctx := e.rawClient.Ctx()
	for {
		rch := watcher.Watch(ctx, e.prefix, clientv3.WithPrefix())
		for wresp := range rch {
			if wresp.Canceled {
				fmt.Println("Canceled...")
				return
			}

			for _, ev := range wresp.Events {
				var evtType EvtType

				// temporarily not used
				switch ev.Type {
				case mvccpb.DELETE:
					evtType = EventTypeDelete
				case mvccpb.PUT:
					if ev.IsCreate() {
						evtType = EventTypeNew
					} else if ev.IsModify() {
						evtType = EventTypeUpdate
					}
				}

				_, ok := e.watchMethodMapping[EvtKey(ev.Kv.Key)]
				if !ok {
					continue
				}

				e.evtCh <- e.watchMethodMapping[EvtKey(ev.Kv.Key)](evtType, ev.Kv)
			}
		}
	}
}
