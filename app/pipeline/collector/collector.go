package collector

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/henrylee2cn/pholcus/runtime/cache"
	"github.com/l-dandelion/gospider/app/pipeline/collector/data"
	"github.com/l-dandelion/gospider/app/spider"
)

type Collector struct {
	*spider.Spider
	DataChan    chan data.DataCell
	FileChan    chan data.FileCell
	dataDocker  []data.DataCell
	outType     string
	dataBatch   uint64
	fileBatch   uint64
	wait        sync.WaitGroup
	sum         [4]uint64 //收集的数据总数[上次输出后文本总数，本次输出后文本总数，上次输出后文件总数，本次输出后文件总数]，非并发安全
	dataSumLock sync.RWMutex
	fileSumLock sync.RWMutex
}

func NewCollector(sp *spider.Spider) *Collector {
	var self = &Collector{}
	self.Spider = sp
	self.outType = cache.Task.OutType
	if cache.Task.DockerCap < 1 {
		cache.Task.DockerCap = 1
	}
	self.DataChan = make(chan data.DataCell, cache.Task.DockerCap)
	self.FileChan = make(chan data.FileCell, cache.Task.DockerCap)
	self.dataDocker = make([]data.DataCell, 0, cache.Task.DockerCap)
	self.sum = [4]uint64{}

	self.dataBatch = 0
	self.fileBatch = 0
	return self
}

func (self *Collector) CollectData(dataCell data.DataCell) error {
	var err error
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("输出协程已终止")
		}
	}()
	self.DataChan <- dataCell
	return err
}

func (self *Collector) CollectFile(fileCell data.FileCell) error {
	var err error
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("输出协程已终止")
		}
	}()
	self.FileChan <- fileCell
	return err
}

func (self *Collector) Stop() {
	go func() {
		defer func() {
			recover()
		}()
		close(self.DataChan)
	}()
	go func() {
		defer func() {
			recover()
		}()
		close(self.DataChan)
	}()
}

func (self *Collector) Start() {
	go func() {
		dataStop := make(chan bool)
		fileStop := make(chan bool)

		go func() {
			defer func() {
				recover()
			}()
			for data := range self.DataChan {
				self.dataDocker = append(self.dataDocker, data)
				if len(self.dataDocker) < cache.Task.DockerCap {
					continue
				}

				self.dataBatch++
				self.outputData()
			}
			self.dataBatch++
			self.outputData()
			close(dataStop)
		}()

		go func() {
			defer func() {
				recover()
			}()

			for file := range self.FileChan {
				atomic.AddUint64(&self.fileBatch, 1)
				self.wait.Add(1)
				go self.outputFile(file)
			}
			close(fileStop)
		}()

		<-dataStop
		<-fileStop

		self.wait.Wait()
		self.Report()
	}()
}

func (self *Collector) resetDataDocker() {
	for _, cell := range self.dataDocker {
		data.PutDataCell(cell)
	}
	self.dataDocker = self.dataDocker[:0]
}

func (self *Collector) dataSum() uint64 {
	self.dataSumLock.RLock()
	defer self.dataSumLock.RUnlock()
	return self.sum[1]
}

func (self *Collector) addDataSum(add uint64) {
	self.dataSumLock.Lock()
	defer self.dataSumLock.Unlock()
	self.sum[0] = self.sum[1]
	self.sum[1] += add
}

func (self *Collector) fileSum() uint64 {
	self.fileSumLock.RLock()
	defer self.fileSumLock.RUnlock()
	return self.sum[3]
}

func (self *Collector) addFileSum(add uint64) {
	self.fileSumLock.Lock()
	defer self.fileSumLock.Unlock()
	self.sum[2] = self.sum[3]
	self.sum[3] += add
}

func (self *Collector) Report() {
	cache.ReportChan <- &cache.Report{
		SpiderName: self.Spider.GetName(),
		Keyin:      self.GetKeyin(),
		DataNum:    self.dataSum(),
		FileNum:    self.fileSum(),
		Time:       time.Since(cache.StartTime),
	}
}
