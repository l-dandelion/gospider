package spider

import (
	"math"
	"sync"
	"time"

	"github.com/henrylee2cn/pholcus/common/util"
	"github.com/henrylee2cn/pholcus/logs"
	"github.com/henrylee2cn/pholcus/runtime/status"
	"github.com/l-dandelion/gospider/app/downloader/request"
	"github.com/l-dandelion/gospider/app/scheduler"
)

const (
	KEYIN       = util.USE_KEYIN
	LIMIT       = math.MaxInt64
	FORCED_STOP = "--主动终止Spider--"
)

type (
	Spider struct {
		Name            string
		Description     string
		PauseTime       int64
		Limit           int64
		Keyin           string
		EnableCookie    bool
		NotDefaultField bool
		Namespace       func(self *Spider) string
		SubNamespace    func(self *Spider, dataCell map[string]interface{}) string
		RuleTree        *RuleTree

		//以下字段系统自动赋值
		id        int //自动分配的SpiderQueue中的索引
		subName   string
		reqMatrix *scheduler.Matrix
		timer     *Timer
		status    int
		lock      sync.RWMutex
		once      sync.Once
	}

	RuleTree struct {
		Root  func(*Context)
		Trunk map[string]*Rule
	}

	Rule struct {
		ItemFields []string                                           //结果字段列表
		ParseFunc  func(*Context)                                     //内容解析函数
		AidFunc    func(*Context, map[string]interface{}) interface{} //通用辅助函数
	}
)

func (self *Spider) GetName() string {
	return self.Name
}

func (self *Spider) GetSubName() string {
	self.once.Do(func() {
		self.subName = self.GetKeyin()
		if len([]rune(self.subName)) > 8 {
			self.subName = util.MakeHash(self.subName)
		}
	})
	return self.subName
}

func (self *Spider) IsStopping() bool {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.status == status.STOP
}

func (self *Spider) tryPanic() {
	if self.IsStopping() {
		panic(FORCED_STOP)
	}
}

func (self *Spider) GetEnableCookie() bool {
	return self.EnableCookie
}

func (self *Spider) GetRule(ruleName string) (*Rule, bool) {
	rule, found := self.RuleTree.Trunk[ruleName]
	return rule, found
}

func (self *Spider) MustGetRule(ruleName string) *Rule {
	return self.RuleTree.Trunk[ruleName]
}

func (self *Spider) GetRules() map[string]*Rule {
	return self.RuleTree.Trunk
}

func (self *Spider) GetDescription() string {
	return self.Description
}

func (self *Spider) GetId() int {
	return self.id
}

func (self *Spider) SetId(id int) *Spider {
	self.id = id
	return self
}

func (self *Spider) GetItemField(rule *Rule, index int) (field string) {
	if index > len(rule.ItemFields)-1 || index < 0 {
		return ""
	}
	return rule.ItemFields[index]
}

func (self *Spider) UpsertItemField(rule *Rule, field string) (index int) {
	for i, v := range rule.ItemFields {
		if v == field {
			return i
		}
	}
	rule.ItemFields = append(rule.ItemFields, field)
	return len(rule.ItemFields) - 1
}

func (self *Spider) GetKeyin() string {
	return self.Keyin
}

func (self *Spider) SetKeyin(keyin string) *Spider {
	self.Keyin = keyin
	return self
}

func (self *Spider) GetLimit() int64 {
	return self.Limit
}

func (self *Spider) SetLimit(limit int64) *Spider {
	self.Limit = limit
	return self
}

func (self *Spider) SetTimer(id string, tol time.Duration, bell *Bell) bool {
	if self.timer == nil {
		self.timer = newTimer()
	}
	return self.timer.set(id, tol, bell)
}

func (self *Spider) RunTimer(id string) bool {
	if self.timer == nil {
		return false
	}
	return self.timer.sleep(id)
}

func (self *Spider) Copy() *Spider {
	ghost := &Spider{}
	ghost.Name = self.Name
	ghost.subName = self.subName

	ghost.RuleTree = &RuleTree{
		Root:  self.RuleTree.Root,
		Trunk: make(map[string]*Rule, len(self.RuleTree.Trunk)),
	}

	for k, v := range self.RuleTree.Trunk {
		ghost.RuleTree.Trunk[k] = new(Rule)

		ghost.RuleTree.Trunk[k].ItemFields = make([]string, len(v.ItemFields))
		copy(ghost.RuleTree.Trunk[k].ItemFields, v.ItemFields)

		ghost.RuleTree.Trunk[k].ParseFunc = v.ParseFunc
		ghost.RuleTree.Trunk[k].AidFunc = v.AidFunc
	}

	ghost.Description = self.Description
	ghost.PauseTime = self.PauseTime
	ghost.EnableCookie = self.EnableCookie
	ghost.Limit = self.Limit
	ghost.Keyin = self.Keyin
	ghost.NotDefaultField = self.NotDefaultField
	ghost.Namespace = self.Namespace
	ghost.SubNamespace = self.SubNamespace
	ghost.timer = self.timer
	ghost.status = self.status

	return ghost
}

func (self *Spider) ReqmatrixInit() *Spider {
	if self.Limit < 0 {
		self.reqMatrix = scheduler.AddMatrix(self.GetName(), self.GetSubName(), self.Limit)
		self.SetLimit(0)
	} else {
		self.reqMatrix = scheduler.AddMatrix(self.GetName(), self.GetSubName(), math.MinInt64)
	}
	return self
}

func (self *Spider) DoHistory(req *request.Request, ok bool) bool {
	return self.reqMatrix.DoHistory(req, ok)
}

func (self *Spider) RequestPush(req *request.Request) {
	self.reqMatrix.Push(req)
}

func (self *Spider) RequestPull() *request.Request {
	return self.reqMatrix.Pull()
}

func (self *Spider) RequestUse() {
	self.reqMatrix.Use()
}

func (self *Spider) RequestFree() {
	self.reqMatrix.Free()
}

func (self *Spider) RequestLen() int {
	return self.reqMatrix.Len()
}

func (self *Spider) TryFlushSuccess() {
	self.reqMatrix.TryFlushSuccess()
}

func (self *Spider) TryFlushFailure() {
	self.reqMatrix.TryFlushFailure()
}

func (self *Spider) Start() {
	defer func() {
		if p := recover(); p != nil {
			logs.Log.Error(" *     Panic [root]: %v\n", p)
		}
		self.lock.Lock()
		self.status = status.RUN
		self.lock.Unlock()
	}()
	self.RuleTree.Root(GetContext(self, nil))
}

func (self *Spider) Stop() {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.status == status.STOP {
		return
	}
	self.status = status.STOP
	if self.timer != nil {
		self.timer.drop()
		self.timer = nil
	}
}

func (self *Spider) CanStop() bool {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.status != status.STOPPED && self.reqMatrix.CanStop()
}

func (self *Spider) Defer() {
	if self.timer != nil {
		self.timer.drop()
		self.timer = nil
	}

	self.reqMatrix.Wait()

	self.reqMatrix.TryFlushFailure()
}

func (self *Spider) OutDefaultField() bool {
	return !self.NotDefaultField
}

func (self *Spider) GetItemFields(rule *Rule) []string {
	return rule.ItemFields
}

func (self *Spider) GetItemFieldIndex(rule *Rule, field string) (index int) {
	for idx, v := range rule.ItemFields {
		if v == field {
			return idx
		}
	}
	return -1
}

func (self *Spider) Register() *Spider {
	self.status = status.STOPPED
	return Species.Add(self)
}
