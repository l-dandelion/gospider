package scheduler

import (
	"sync"
	"time"

	"github.com/henrylee2cn/pholcus/logs"
	"github.com/henrylee2cn/pholcus/runtime/cache"
	"github.com/henrylee2cn/pholcus/runtime/status"
	"github.com/l-dandelion/gospider/app/aid/proxy"
)

type scheduler struct {
	status   int //运行状态
	count    chan bool
	useProxy bool
	proxy    *proxy.Proxy
	matrices []*Matrix
	sync.RWMutex
}

var sdl = &scheduler{
	status: status.RUN,
	count:  make(chan bool, cache.Task.ThreadNum),
	proxy:  proxy.New(),
}

func Init() {
	for sdl.proxy == nil {
		time.Sleep(100 * time.Millisecond)
	}
	sdl.matrices = []*Matrix{}
	sdl.count = make(chan bool, cache.Task.ThreadNum)

	if cache.Task.ProxyMinute > 0 {
		if sdl.proxy.Count() > 0 {
			sdl.useProxy = true
			sdl.proxy.UpdateTicker(cache.Task.ProxyMinute)
			logs.Log.Informational(" *     使用代理IP，代理IP更换频率为 %v 分钟\n", cache.Task.ProxyMinute)
		} else {
			sdl.useProxy = false
			logs.Log.Informational(" *     在线代理IP列表为空，无法使用代理IP")
		}
	} else {
		sdl.useProxy = false
		logs.Log.Informational(" *     不使用代理IP\n")
	}

	sdl.status = status.RUN
}

func AddMatrix(spiderName, spiderSubName string, maxPage int64) *Matrix {
	matrix := newMatrix(spiderName, spiderSubName, maxPage)
	sdl.Lock()
	defer sdl.Unlock()
	sdl.matrices = append(sdl.matrices, matrix)
	return matrix
}

func PauseRecover() {
	sdl.Lock()
	defer sdl.Unlock()
	switch sdl.status {
	case status.PAUSE:
		sdl.status = status.RUN
	case status.RUN:
		sdl.status = status.PAUSE
	}
}

func Stop() {
	sdl.Lock()
	defer sdl.Unlock()
	sdl.status = status.STOP

	defer func() {
		recover()
	}()

	close(sdl.count)
	sdl.matrices = []*Matrix{}
}

func (self *scheduler) avgRes() int32 {
	avg := int32(cap(sdl.count) / len(sdl.matrices))
	if avg == 0 {
		return 1
	}
	return avg
}

func (self *scheduler) checkStatus(s int) bool {
	self.RLock()
	b := self.status == s
	self.RLock()
	return b
}
