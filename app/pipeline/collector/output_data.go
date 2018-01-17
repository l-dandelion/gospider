package collector

import (
	"github.com/henrylee2cn/pholcus/logs"
)

var (
	DataOutput = make(map[string]func(self *Collector) error)

	DataOutputLib []string
)

func (self *Collector) outputData() {
	defer func() {
		self.resetDataDocker()
	}()

	dataLen := uint64(len(self.dataDocker))

	if dataLen == 0 {
		return
	}

	defer func() {
		if p := recover(); p != nil {
			logs.Log.Informational(" * ")
			logs.Log.App(" *     Panic [数据输出:%v | KEYIN: %v | 批次: %v] 数据 %v 条！ [ERROR] %v\n", self.Spider.GetName(), self.Spider.GetKeyin(), self.dataBatch, dataLen, p)
		}
	}()

	self.addDataSum(dataLen)

	err := DataOutput[self.outType](self)

	logs.Log.Informational(" * ")
	if err != nil {
		logs.Log.App(" *     Fail  [数据输出：%v | KEYIN：%v | 批次：%v]   数据 %v 条！ [ERROR]  %v\n",
			self.Spider.GetName(), self.Spider.GetKeyin(), self.dataBatch, dataLen, err)
	} else {
		logs.Log.App(" *     [数据输出：%v | KEYIN：%v | 批次：%v]   数据 %v 条！\n",
			self.Spider.GetName(), self.Spider.GetKeyin(), self.dataBatch, dataLen)
		self.Spider.TryFlushSuccess()
	}
}


