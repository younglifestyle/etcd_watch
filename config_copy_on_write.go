package main

import (
	"encoding/json"
)

// 此处，可以将多个key，拼接成到一个struct中，避免使用Map
// 当避免不了使用Map时，建议使用Copy On Write方式进行更新数据
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

