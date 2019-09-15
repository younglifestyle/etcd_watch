package main

import (
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"sync"
	"time"
)

// EvtType event type
type EvtType int

// EvtSrc event src
type EvtSrc int

// EvtKeyStr event key string
type EvtKeyStr string

// Evt event
type Evt struct {
	Src   EvtSrc
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
	// DefaultRequestTimeout default request timeout
	DefaultRequestTimeout = 10 * time.Second
	// DefaultSlowRequestTime default slow request time
	DefaultSlowRequestTime = time.Second * 1
)

// EtcdStore etcd store impl
type EtcdStore struct {
	sync.RWMutex

	prefix       string
	keyStoreDirs map[string]string

	evtCh              chan *Evt
	watchMethodMapping map[EvtKeyStr]func(EvtType, *mvccpb.KeyValue) *Evt

	rawClient *clientv3.Client
}

// 设计思想 :
// 1、判断nil，非空无数据等操作，由调用者自己控制，均不设计此类判断在库中
// 2、公共函数需要添加注释

// NewEtcdStore create a etcd store
func NewEtcdStore(etcdAddrs []string, prefix string, basicAuth BasicAuth, watchHandle map[EvtKeyStr]func(EvtType, *mvccpb.KeyValue) *Evt) (*EtcdStore, error) {

	store := &EtcdStore{
		prefix:             prefix,
		watchMethodMapping: watchHandle,
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

	cli, err := clientv3.New(*config)

	if err != nil {
		return nil, err
	}

	store.rawClient = cli

	return store, nil
}

// Watch watch event from etcd
func (e *EtcdStore) Watch(evtCh chan *Evt, stopCh chan bool) error {
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

				e.evtCh <- e.watchMethodMapping[EvtKeyStr(ev.Kv.Key)](evtType, ev.Kv)
			}
		}
	}
}
