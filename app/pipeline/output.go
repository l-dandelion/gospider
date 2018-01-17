package pipeline

import (
	"github.com/henrylee2cn/pholcus/common/kafka"
	"github.com/henrylee2cn/pholcus/common/mgo"
	"github.com/henrylee2cn/pholcus/common/mysql"
	"github.com/henrylee2cn/pholcus/runtime/cache"
	"github.com/l-dandelion/gospider/app/pipeline/collector"
	"sort"
)

func init() {
	for out, _ := range collector.DataOutput {
		collector.DataOutputLib = append(collector.DataOutputLib, out)
	}
	sort.Strings(collector.DataOutputLib)
}

func RefreshOutput() {
	switch cache.Task.OutType {
	case "mgo":
		mgo.Refresh()
	case "mysql":
		mysql.Refresh()
	case "kafka":
		kafka.Refresh()
	}
}
