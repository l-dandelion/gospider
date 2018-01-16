package proxy

import (
	"sync"
	"time"
)

type ProxyForHost struct {
	curIndex  int
	proxys    []string
	timedelay []time.Duration
	isEcho    bool
	sync.Mutex
}

func (self *ProxyForHost) Len() int {
	return len(self.proxys)
}

func (self *ProxyForHost) Less(i, j int) bool {
	return self.timedelay[i] < self.timedelay[j]
}

func (self *ProxyForHost) Swap(i, j int) {
	self.proxys[i], self.proxys[j] = self.proxys[j], self.proxys[i]
	self.timedelay[i], self.timedelay[j] = self.timedelay[j], self.timedelay[i]
}
