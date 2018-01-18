package crawler

import (
	"sync"
	"time"

	"github.com/henrylee2cn/pholcus/config"
	"github.com/henrylee2cn/pholcus/runtime/status"
)

type (
	CrawlerPool interface {
		Reset(spiderName int) int
		Use() Crawler
		Free(Crawler)
		Stop()
	}

	cq struct {
		capacity int
		count    int
		usable   chan Crawler
		all      []Crawler
		status   int
		sync.RWMutex
	}
)

func NewCrawlerPool() CrawlerPool {
	return &cq{
		status: status.RUN,
		all:    make([]Crawler, 0, config.CRAWLS_CAP),
	}
}

func (self *cq) Reset(spiderNum int) int {
	self.Lock()
	defer self.Unlock()
	var wantNum int
	if spiderNum < config.CRAWLS_CAP {
		wantNum = spiderNum
	} else {
		wantNum = config.CRAWLS_CAP
	}
	if wantNum <= 0 {
		wantNum = 1
	}
	self.capacity = wantNum
	self.count = 0
	self.usable = make(chan Crawler, wantNum)
	for _, crawler := range self.all {
		if self.count < self.capacity {
			self.usable <- crawler
			self.count++
		}
	}
	self.status = status.RUN
	return wantNum
}

func (self *cq) Use() Crawler {
	var crawler Crawler
	for {
		self.Lock()
		if self.status == status.STOP {
			self.Unlock()
			return nil
		}
		select {
		case crawler = <-self.usable:
			self.Unlock()
			return crawler
		default:
			if self.count < self.capacity {
				crawler = New(self.count)
				self.all = append(self.all, crawler)
				self.count++
				self.Unlock()
				return crawler
			}
		}
		self.Unlock()
		time.Sleep(time.Second)
	}
}

func (self *cq) Free(crawler Crawler) {
	self.RLock()
	defer self.RUnlock()
	if self.status == status.STOP || !crawler.CanStop() {
		return
	}
	self.usable <- crawler
}

func (self *cq) Stop() {
	self.Lock()
	if self.status == status.STOP{
		self.Unlock()
		return
	}
	self.status = status.STOP
	close(self.usable)
	self.usable = nil
	self.Unlock()

	for _, crawler := range self.all {
		crawler.Stop()
	}
}


