package spider

import (
	"github.com/henrylee2cn/pholcus/logs"
	"sync"
	"time"
)

type Timer struct {
	setting map[string]*Clock
	closed  bool
	sync.RWMutex
}

type (
	Clock struct {
		id    string
		typ   int
		tol   time.Duration
		bell  *Bell
		timer *time.Timer
	}
	Bell struct {
		Hour int
		Min  int
		Sec  int
	}
)

const (
	//闹钟
	A = iota
	//倒计时
	T
)

func newClock(id string, tol time.Duration, bell *Bell) (*Clock, bool) {
	if tol <= 0 {
		return nil, false
	}
	if bell == nil {
		return &Clock{
			id:    id,
			typ:   T,
			tol:   tol,
			timer: newT(),
		}, true
	}
	if !(bell.Hour >= 0 && bell.Hour < 24 && bell.Min >= 0 && bell.Min < 60 && bell.Sec >= 0 && bell.Sec < 60) {
		return nil, false
	}
	return &Clock{
		id:    id,
		typ:   A,
		tol:   tol,
		bell:  bell,
		timer: newT(),
	}, true
}

func (self *Clock) duration() time.Duration {
	switch self.typ {
	case A:
		t := time.Now()
		year, month, day := t.Date()
		bell := time.Date(year, month, day, self.bell.Hour, self.bell.Min, self.bell.Sec, 0, time.Local)
		if bell.Before(t) {
			bell = bell.Add(time.Hour * 24 * self.tol)
		} else {
			bell = bell.Add(time.Hour * 24 * (self.tol - 1))
		}
		return bell.Sub(t)
	case T:
		return self.tol
	}
	return 0
}

func (self *Clock) sleep() {
	d := self.duration()
	self.timer.Reset(d)
	t0 := time.Now()
	logs.Log.Critical("**************** ...定时器 <%s> 睡眠 %v,计划 %v 醒来 ...*****************", self.id, d, t0.Add(d).Format("2016-01-02 15:04:05"))
	<-self.timer.C
	t1 := time.Now()
	logs.Log.Critical("****************** ...定时器 <%s> 在 %v 醒来，实际睡眠 %v ... ***********", self.id, t1.Format("2006-01-02 15:04:05"), t1.Sub(t0))
}

func (self *Clock) wake() {
	self.timer.Reset(0)
}

func newT() *time.Timer {
	t := time.NewTimer(0)
	<-t.C
	return t
}

func newTimer() *Timer {
	return &Timer{
		setting: make(map[string]*Clock),
	}
}

func (self *Timer) sleep(id string) bool {
	self.RLock()
	if self.closed {
		self.RUnlock()
		return false
	}

	c, ok := self.setting[id]
	self.RUnlock()
	if !ok {
		return false
	}

	c.sleep()

	self.RLock()
	defer self.RUnlock()
	if self.closed {
		return false
	}
	_, ok = self.setting[id]
	return ok
}

func (self *Timer) set(id string, tol time.Duration, bell *Bell) bool {
	self.Lock()
	defer self.Unlock()
	if self.closed {
		logs.Log.Critical("************************ ……设置定时器 [%s] 失败，定时系统已关闭 ……************************", id)
		return false
	}
	c, ok := newClock(id, tol, bell)
	if !ok {
		logs.Log.Critical("************************ ……设置定时器 [%s] 失败，参数不正确 ……************************", id)
		return ok
	}
	self.setting[id] = c
	logs.Log.Critical("************************ ……设置定时器 [%s] 成功 ……************************", id)
	return ok
}

func (self *Timer) drop() {
	self.Lock()
	defer self.Unlock()
	self.closed = true
	for _, c := range self.setting {
		//可以让c停止sleep
		c.wake()
	}
	self.setting = make(map[string]*Clock)
}
