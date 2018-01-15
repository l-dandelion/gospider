package surfer

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

type (
	Request interface {
		GetUrl() string
		GetMethod() string
		GetPostData() string
		GetHeader() http.Header
		GetEnableCookie() bool
		GetDialTimeout() time.Duration
		GetConnTimeout() time.Duration
		GetTryTimes() int
		GetRetryPause() time.Duration
		GetProxy() string
		GetRedirectTimes() int
		GetDownloaderID() int
	}

	//默认实现的Request
	DefaultRequest struct {
		Url           string
		Method        string
		Header        http.Header
		EnableCookie  bool
		PostData      string
		DialTimeout   time.Duration
		ConnTimeout   time.Duration
		TryTimes      int
		RetryPause    time.Duration
		RedirectTimes int
		Proxy         string

		// 指定下载器ID
		// 0为Surf高并发下载器，各种控制功能齐全
		// 1为PhantomJS下载器，特点破防力强，速度慢，低并发
		DownloaderID int

		//保证prepare只调用一次
		once sync.Once
	}
)

const (
	SurfID             = 0               // Surf下载器标识符
	PhomtomJsID        = 1               // PhomtomJs下载器标识符
	DefaultMethod      = "GET"           // 默认请求方法
	DefaultDialTimeout = 2 * time.Minute // 默认请求服务器超时
	DefaultConnTimeout = 2 * time.Minute // 默认下载超时
	DefaultTryTimes    = 3               // 默认最大下载次数
	DefaultRetryPause  = 2 * time.Second // 默认重新下载前停顿时长
)

func (self *DefaultRequest) prepare() {
	if self.Method == "" {
		self.Method = DefaultMethod
	}
	self.Method = strings.ToUpper(self.Method)

	if self.Header == nil {
		self.Header = make(http.Header)
	}

	if self.DialTimeout < 0 {
		self.DialTimeout = 0
	} else if self.DialTimeout == 0 {
		self.DialTimeout = DefaultDialTimeout
	}

	if self.ConnTimeout < 0 {
		self.ConnTimeout = 0
	} else if self.ConnTimeout == 0 {
		self.ConnTimeout = DefaultConnTimeout
	}

	if self.TryTimes == 0 {
		self.TryTimes = DefaultTryTimes
	}

	if self.RetryPause <= 0 {
		self.RetryPause = DefaultRetryPause
	}

	if self.DownloaderID != PhomtomJsID {
		self.DownloaderID = SurfID
	}
}

// url
func (self *DefaultRequest) GetUrl() string {
	self.once.Do(self.prepare)
	return self.Url
}

// GET POST POST-M HEAD
func (self *DefaultRequest) GetMethod() string {
	self.once.Do(self.prepare)
	return self.Method
}

// POST values
func (self *DefaultRequest) GetPostData() string {
	self.once.Do(self.prepare)
	return self.PostData
}

// http header
func (self *DefaultRequest) GetHeader() http.Header {
	self.once.Do(self.prepare)
	return self.Header
}

// enable http cookies
func (self *DefaultRequest) GetEnableCookie() bool {
	self.once.Do(self.prepare)
	return self.EnableCookie
}

// dial tcp: i/o timeout
func (self *DefaultRequest) GetDialTimeout() time.Duration {
	self.once.Do(self.prepare)
	return self.DialTimeout
}

// WSARecv tcp: i/o timeout
func (self *DefaultRequest) GetConnTimeout() time.Duration {
	self.once.Do(self.prepare)
	return self.ConnTimeout
}

// the max times of download
func (self *DefaultRequest) GetTryTimes() int {
	self.once.Do(self.prepare)
	return self.TryTimes
}

// the pause time of retry
func (self *DefaultRequest) GetRetryPause() time.Duration {
	self.once.Do(self.prepare)
	return self.RetryPause
}

// the download ProxyHost
func (self *DefaultRequest) GetProxy() string {
	self.once.Do(self.prepare)
	return self.Proxy
}

// max redirect times
func (self *DefaultRequest) GetRedirectTimes() int {
	self.once.Do(self.prepare)
	return self.RedirectTimes
}

// select Surf ro PhomtomJS
func (self *DefaultRequest) GetDownloaderID() int {
	self.once.Do(self.prepare)
	return self.DownloaderID
}
