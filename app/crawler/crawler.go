package crawler

import (
	"bytes"
	"math/rand"
	"runtime"
	"time"

	"github.com/henrylee2cn/pholcus/logs"
	"github.com/henrylee2cn/pholcus/runtime/cache"
	"github.com/l-dandelion/gospider/app/downloader"
	"github.com/l-dandelion/gospider/app/downloader/request"
	"github.com/l-dandelion/gospider/app/pipeline"
	"github.com/l-dandelion/gospider/app/spider"
)

type (
	Crawler interface {
		Init(*spider.Spider) Crawler
		Run()
		Stop()
		CanStop() bool
		GetId() int
	}

	crawler struct {
		*spider.Spider                 //执行采集的规则
		downloader.Downloader          //全局公用的下载器
		pipeline.Pipeline              //结果收集与输出管道
		id                    int      //引擎id
		pause                 [2]int64 //请求间隔
	}
)

func New(id int) Crawler {
	return &crawler{
		id:         id,
		Downloader: downloader.SurfDownloader,
	}
}

func (self *crawler) Init(sp *spider.Spider) Crawler {
	self.Spider = sp.ReqmatrixInit()
	self.Pipeline = pipeline.New(sp)
	self.pause[0] = sp.PauseTime / 2
	if self.pause[0] > 0 {
		self.pause[1] = self.pause[0] * 3
	} else {
		self.pause[1] = 1
	}
	return self
}

func (self *crawler) Run() {
	self.Pipeline.Start()

	c := make(chan bool)
	go func() {
		self.run()
		close(c)
	}()
	self.Spider.Start()
	<-c
	self.Pipeline.Stop()
}

func (self *crawler) Stop() {
	self.Spider.Stop()
	self.Pipeline.Stop()
}

func (self *crawler) run() {
	for {
		req := self.GetOne()
		if req != nil {
			if self.Spider.CanStop() {
				break
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}
		self.UseOne()
		go func() {
			defer func() {
				self.FreeOne()
			}()
			logs.Log.Debug(" *     Start: %v", req.GetUrl())
			self.Process(req)
		}()

		self.sleep()
	}
	self.Spider.Defer()
}

func (self *crawler) Process(req *request.Request) {
	var (
		downUrl = req.GetUrl()
		sp      = self.Spider
	)

	defer func() {
		if p := recover(); p != nil {
			if sp.IsStopping() {
				return
			}
			if sp.DoHistory(req, false) {
				cache.PageFailCount()
			}

			stack := make([]byte, 4<<10)
			length := runtime.Stack(stack, true)
			start := bytes.Index(stack, []byte("/src/runtime/panic.go"))
			stack = stack[start:length]
			start = bytes.Index(stack, []byte("\n")) + 1
			stack = stack[start:]
			if end := bytes.Index(stack, []byte("\ngoroutine ")); end != -1 {
				stack = stack[:end]
			}
			stack = bytes.Replace(stack, []byte("\n"), []byte("\r\n"), -1)
			logs.Log.Error(" *     Panic  [process][%s]: %s\r\n[TRACE]\r\n%s", downUrl, p, stack)
		}
	}()

	var ctx = self.Downloader.Download(sp, req)
	if err := ctx.GetError(); err != nil {
		if sp.DoHistory(req, false) {
			cache.PageFailCount()
		}
		logs.Log.Error(" *     Fail  [download][%v]: %v\n", downUrl, err)
		return
	}
	ctx.Parse(req.GetUrl())

	for _, f := range ctx.PullFiles() {
		if self.Pipeline.CollectFile(f) != nil {
			break
		}
	}

	for _, item := range ctx.PullItems() {
		if self.Pipeline.CollectData(item) != nil {
			break
		}
	}

	sp.DoHistory(req, true)

	cache.PageSuccCount()

	logs.Log.Informational(" *     Success: %v\n", downUrl)
	spider.PutContext(ctx)
}

func (self *crawler) sleep() {
	sleeptime := self.pause[0] + rand.Int63n(self.pause[1])
	time.Sleep(time.Duration(sleeptime) * time.Millisecond)
}

func (self *crawler) GetOne() *request.Request {
	return self.Spider.RequestPull()
}

func (self *crawler) UseOne() {
	self.Spider.RequestUse()
}

func (self *crawler) FreeOne() {
	self.Spider.RequestFree()
}

func (self *crawler) SetId(id int) {
	self.id = id
}

func (self *crawler) GetId() int {
	return self.id
}
