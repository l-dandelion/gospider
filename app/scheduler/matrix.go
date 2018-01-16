package scheduler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/henrylee2cn/pholcus/logs"
	"github.com/henrylee2cn/pholcus/runtime/cache"
	"github.com/henrylee2cn/pholcus/runtime/status"
	"github.com/l-dandelion/gospider/app/aid/history"
	"github.com/l-dandelion/gospider/app/downloader/request"
)

type Matrix struct {
	maxPage         int64                       //最大采集页数
	resCount        int32                       //资源使用情况计数
	spiderName      string                      //所属spider
	reqs            map[int][]*request.Request  //[优先级]队列,优先级默认为0
	priorities      []int                       //优先级顺序，从低到高
	history         history.Historier           //历史记录
	tempHistory     map[string]bool             //临时记录 [reqUnique(url + method)] true
	failures        map[string]*request.Request //历史及本次失败请求
	tempHistoryLock sync.RWMutex
	failureLock     sync.Mutex
	sync.Mutex
}

func newMatrix(spiderName, spiderSubName string, maxPage int64) *Matrix {
	matrix := &Matrix{
		spiderName:  spiderName,
		maxPage:     maxPage,
		reqs:        make(map[int][]*request.Request),
		priorities:  []int{},
		history:     history.New(spiderName, spiderSubName),
		tempHistory: make(map[string]bool),
		failures:    make(map[string]*request.Request),
	}
	if cache.Task.Mode != status.SERVER {
		matrix.history.ReadSuccess(cache.Task.OutType, cache.Task.SuccessInherit)
		matrix.history.ReadFailure(cache.Task.OutType, cache.Task.FailureInherit)
		matrix.setFailures(matrix.history.PullFailure())
	}
	return matrix
}

func (self *Matrix) Push(req *request.Request) {
	self.Lock()
	defer self.Unlock()

	if sdl.checkStatus(status.STOP) {
		return
	}

	waited := false

	for sdl.checkStatus(status.PAUSE) {
		waited = true
		time.Sleep(time.Second)
	}

	if waited && sdl.checkStatus(status.STOP) {
		return
	}

	waited = false
	for atomic.LoadInt32(&self.resCount) > sdl.avgRes() {
		waited = true
		time.Sleep(100 * time.Millisecond)
	}
	if waited && sdl.checkStatus(status.STOP) {
		return
	}

	if !req.IsReloadable() {
		if self.hasHistory(req.Unique()) {
			return
		}
		self.insertTempHistory(req.Unique())
	}

	var priority = req.GetPriority()

	if _, found := self.reqs[priority]; !found {
		self.priorities = append(self.priorities, priority)
		sort.Ints(self.priorities)
		self.reqs[priority] = []*request.Request{}
	}

	self.reqs[priority] = append(self.reqs[priority], req)

	atomic.AddInt64(&self.maxPage, 1)
}

func (self *Matrix) Pull() (req *request.Request) {
	self.Lock()
	defer self.Unlock()

	if !sdl.checkStatus(status.RUN) {
		return
	}

	for i := len(self.reqs) - 1; i >= 0; i-- {
		idx := self.priorities[i]
		if len(self.reqs[idx]) > 0 {
			req = self.reqs[idx][0]
			self.reqs[idx] = self.reqs[idx][1:]
			if sdl.useProxy {
				req.SetProxy(sdl.proxy.GetOne(req.GetUrl()))
			} else {
				req.SetProxy("")
			}
			return
		}
	}
	return
}

func (self *Matrix) Use() {
	defer func() {
		recover()
	}()
	sdl.count <- true
	atomic.AddInt32(&self.resCount, 1)
}

func (self *Matrix) Free() {
	<-sdl.count
	atomic.AddInt32(&self.resCount, -1)
}

func (self *Matrix) DoHistory(req *request.Request, ok bool) bool {
	if !req.IsReloadable() {
		self.tempHistoryLock.Lock()
		delete(self.tempHistory, req.Unique())
		self.tempHistoryLock.Unlock()
		if ok {
			self.history.UpsertSuccess(req.Unique())
			return false
		}
	}

	if ok {
		return false
	}

	self.failureLock.Lock()
	defer self.failureLock.Unlock()
	if _, ok := self.failures[req.Unique()]; !ok {
		self.failures[req.Unique()] = req
		logs.Log.Informational(" *     + 失败请求: [%v]\n", req.GetUrl())
		return true
	}

	self.history.UpsertFailure(req)
	return false
}

func (self *Matrix) CanStop() bool {
	if sdl.checkStatus(status.STOP) {
		return true
	}
	if self.maxPage >= 0 {
		return true
	}
	if atomic.LoadInt32(&self.resCount) != 0 {
		return false
	}
	if self.Len() > 0 {
		return false
	}

	self.failureLock.Lock()
	defer self.failureLock.Unlock()

	if len(self.failures) > 0 {
		var goon bool
		for reqUnique, req := range self.failures {
			if req == nil {
				continue
			}
			self.failures[reqUnique] = nil
			goon = true
			logs.Log.Informational(" *     - 失败请求: [%v]\n", req.GetUrl())
			self.Push(req)
		}
		if goon {
			return false
		}
	}
	return true
}

func (self *Matrix) TryFlushSuccess() {
	if cache.Task.Mode != status.SERVER && cache.Task.SuccessInherit {
		self.history.FlushSuccess(cache.Task.OutType)
	}
}

func (self *Matrix) TryFlushFailure() {
	if cache.Task.Mode != status.SERVER && cache.Task.FailureInherit {
		self.history.FlushFailure(cache.Task.OutType)
	}
}

func (self *Matrix) Wait() {
	if sdl.checkStatus(status.STOP) {
		return
	}

	for atomic.LoadInt32(&self.resCount) != 0 {
		time.Sleep(500 * time.Millisecond)
	}
}

func (self *Matrix) Len() int {
	self.Lock()
	defer self.Unlock()
	var l int
	for _, reqs := range self.reqs {
		l += len(reqs)
	}
	return l
}

func (self *Matrix) hasHistory(reqUnique string) bool {
	if self.history.HasSuccess(reqUnique) {
		return true
	}
	self.tempHistoryLock.RLock()
	has := self.tempHistory[reqUnique]
	self.tempHistoryLock.RUnlock()
	return has
}

func (self *Matrix) insertTempHistory(reqUnique string) {
	self.tempHistoryLock.Lock()
	self.tempHistory[reqUnique] = true
	self.tempHistoryLock.Unlock()
}

func (self *Matrix) setFailures(reqs map[string]*request.Request) {
	self.failureLock.Lock()
	defer self.failureLock.Unlock()
	for key, req := range reqs {
		self.failures[key] = req
		logs.Log.Informational(" *     + 失败请求: [%v]\n", req.GetUrl())
	}
}
