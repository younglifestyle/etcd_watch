package main

import (
	"encoding/json"
)

// https://chunlife.top/2019/09/03/copy-on-write%E6%8A%80%E6%9C%AF/
func copyExample(originData map[uint64]*testS) map[uint64]*testS {
	values := make(map[uint64]*testS)
	for key, value := range originData {
		var tmpData *testS
		bytes, _ := json.Marshal(value)
		json.Unmarshal(bytes, tmpData)

		values[key] = tmpData
	}

	return values
}

