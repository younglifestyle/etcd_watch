package main

import (
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

var (
	keyPrefix = "C:/Program Files/Git/logagent/con1"
	cfgData   = &testS{}
)

func main() {
	var (
		etcdAddr    = []string{"http://127.0.0.1:2379"}
		watchHandle = make(map[EvtKey]func(EvtType, *mvccpb.KeyValue) *Evt)
	)

	watchHandle[EvtKey(keyPrefix)] = doWatchTest
	etcdStore, err := NewEtcdStore(etcdAddr, keyPrefix, BasicAuth{}, watchHandle)
	if err != nil {
		fmt.Println("error :", err)
		return
	}

	fmt.Println("start...")

	evtCh := make(chan *Evt)
	go etcdStore.Watch(evtCh)

	go readyToReceiveWatchEvent(evtCh)

	select {}
}

func readyToReceiveWatchEvent(evtCh chan *Evt) {
	for {
		evt := <-evtCh

		if evt.Key == keyPrefix {
			cfgData, _ = evt.Value.(*testS)
			fmt.Println("test :", cfgData.One)
		} else {
			fmt.Println("key :", evt.Key)
		}
	}
}

type testS struct {
	One  string `json:"one"`
	Test int    `json:"test"`
}

// 可以自定义数据处理函数，若不自定义，则使用默认处理函数，返回byte数组数据
func doWatchTest(evtType EvtType, kv *mvccpb.KeyValue) *Evt {

	value := &testS{}
	err := json.Unmarshal(kv.Value, value)
	if err != nil {
		fmt.Println("error ...", err)
	}

	return &Evt{
		Type:  evtType,
		Key:   string(kv.Key),
		Value: value,
	}
}
