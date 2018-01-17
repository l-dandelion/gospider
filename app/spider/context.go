package spider

import (
	"bytes"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/net/html/charset"

	"github.com/henrylee2cn/pholcus/common/goquery"
	"github.com/henrylee2cn/pholcus/common/util"
	"github.com/henrylee2cn/pholcus/logs"
	"github.com/l-dandelion/gospider/app/downloader/request"
	"github.com/l-dandelion/gospider/app/pipeline/collector/data"
)

type Context struct {
	spider   *Spider           //规则
	Request  *request.Request  //原始请求
	Response *http.Response    //响应流，其中URL拷贝自*request.Request
	text     []byte            //下载内容Body的字节流格式
	dom      *goquery.Document //下载内容Body为html时，可转换为dom的对象
	items    []data.DataCell   //存放以文本形式输出的结果数据
	files    []data.FileCell   //存放欲直接输出的文件("Name": string; "Body": io.ReadCloser)
	err      error             //错误标记
	sync.Mutex
}

var (
	contextPool = &sync.Pool{
		New: func() interface{} {
			return &Context{
				items: []data.DataCell{},
				files: []data.FileCell{},
			}
		},
	}
)

func GetContext(sp *Spider, req *request.Request) *Context {
	ctx := contextPool.Get().(*Context)
	ctx.spider = sp
	ctx.Request = req
	return ctx
}

func PutContext(ctx *Context) {
	ctx.items = ctx.items[:0]
	ctx.files = ctx.files[:0]
	ctx.spider = nil
	ctx.Request = nil
	ctx.Response = nil
	ctx.text = nil
	ctx.dom = nil
	ctx.err = nil
	contextPool.Put(ctx)
}

func (self *Context) SetResponse(resp *http.Response) *Context {
	self.Response = resp
	return self
}

func (self *Context) SetError(err error) {
	self.err = err
}

func (self *Context) AddQueue(req *request.Request) *Context {
	self.spider.tryPanic()

	err := req.SetSpiderName(self.spider.GetName()).
		SetEnableCookie(self.spider.GetEnableCookie()).
		Prepare()
	if err != nil {
		logs.Log.Error(err.Error())
		return self
	}

	if req.GetReferer() == "" && self.Response != nil {
		req.SetReferer(self.GetUrl())
	}
	self.spider.RequestPush(req)
	return self
}

func (self *Context) JsAddQueue(jreq map[string]interface{}) *Context {
	self.spider.tryPanic()
	req := &request.Request{}
	u, ok := jreq["Url"].(string)
	if !ok {
		return self
	}
	req.Url = u
	req.Rule, _ = jreq["Rule"].(string)
	req.Method, _ = jreq["Method"].(string)
	req.Header = http.Header{}
	if header, ok := jreq["Header"].(map[string]interface{}); ok {
		for k, values := range header {
			if vals, ok := values.([]string); ok {
				for _, v := range vals {
					req.Header.Add(k, v)
				}
			}
		}
	}
	req.PostData, _ = jreq["PostData"].(string)
	req.Reloadable, _ = jreq["Reloadable"].(bool)
	if t, ok := jreq["DialTimeout"].(int64); ok {
		req.DialTimeout = time.Duration(t)
	}
	if t, ok := jreq["ConnTimeout"].(int64); ok {
		req.ConnTimeout = time.Duration(t)
	}
	if t, ok := jreq["RetryPause"].(int64); ok {
		req.RetryPause = time.Duration(t)
	}
	if t, ok := jreq["TryTimes"].(int64); ok {
		req.TryTimes = int(t)
	}
	if t, ok := jreq["RedirectTimes"].(int64); ok {
		req.RedirectTimes = int(t)
	}
	if t, ok := jreq["Priority"].(int64); ok {
		req.Priority = int(t)
	}
	if t, ok := jreq["DownloaderID"].(int64); ok {
		req.DownloaderID = int(t)
	}
	if t, ok := jreq["Temp"].(map[string]interface{}); ok {
		req.Temp = t
	}
	err := req.
		SetSpiderName(self.spider.GetName()).
		SetEnableCookie(self.spider.GetEnableCookie()).
		Prepare()

	if err != nil {
		logs.Log.Error(err.Error())
		return self
	}

	if req.GetReferer() == "" && self.Response != nil {
		req.SetReferer(self.GetUrl())
	}
	self.spider.RequestPush(req)
	return self
}

func (self *Context) Output(item interface{}, ruleName ...string) {
	_ruleName, rule, found := self.getRule(ruleName...)
	if !found {
		logs.Log.Error("蜘蛛 %s 调用Output()时，指定的规则名不存在!", self.spider.GetName())
		return
	}
	var _item map[string]interface{}
	switch item2 := item.(type) {
	case map[int]interface{}:
		_item = self.CreateItem(item2, _ruleName)
	case request.Temp:
		for k := range item2 {
			self.spider.UpsertItemField(rule, k)
		}
		_item = item2
	case map[string]interface{}:
		for k := range item2 {
			self.spider.UpsertItemField(rule, k)
		}
		_item = item2
	}
	self.Lock()
	if self.spider.NotDefaultField {
		self.items = append(self.items, data.GetDataCell(_ruleName, _item, "", "", ""))
	} else {
		self.items = append(self.items, data.GetDataCell(_ruleName, _item, self.GetUrl(), self.GetReferer(), time.Now().Format("2006-01-02 15:04:05")))
	}
	self.Unlock()
}

func (self *Context) FileOutput(nameOrExt ...string) {
	bytes, err := ioutil.ReadAll(self.Response.Body)
	self.Response.Body.Close()
	if err != nil {
		panic(err.Error())
		return
	}

	_, s := path.Split(self.GetUrl())
	n := strings.Split(s, "?")[0]

	var baseName, ext string

	if len(nameOrExt) > 0 {
		p, n := path.Split(nameOrExt[0])
		ext = path.Ext(n)
		if baseName2 := strings.TrimSuffix(n, ext); baseName2 != "" {
			baseName = p + baseName2
		}
	}
	if baseName == "" {
		baseName = strings.TrimSuffix(n, path.Ext(n))
	}
	if ext == "" {
		ext = path.Ext(n)
	}
	if ext == "" {
		ext = ".html"
	}

	self.Lock()
	self.files = append(self.files, data.GetFileCell(self.GetRuleName(), baseName+ext, bytes))
	self.Unlock()
}

func (self *Context) CreateItem(item map[int]interface{}, ruleName ...string) map[string]interface{} {
	_, rule, found := self.getRule(ruleName...)
	if !found {
		logs.Log.Error("蜘蛛 %s 调用CreatItem()时，指定的规则名不存在！", self.spider.GetName())
		return nil
	}
	var item2 = make(map[string]interface{}, len(item))
	for k, v := range item {
		field := self.spider.GetItemField(rule, k)
		item2[field] = v
	}
	return item2
}

func (self *Context) GetRuleName() string {
	return self.Request.GetRuleName()
}

func (self *Context) getRule(ruleName ...string) (name string, rule *Rule, found bool) {
	if len(ruleName) == 0 {
		if self.Response == nil {
			return
		}
		name = self.GetRuleName()
	} else {
		name = ruleName[0]
	}
	rule, found = self.spider.GetRule(name)
	return
}

func (self *Context) GetUrl() string {
	return self.Request.Url
}

func (self *Context) GetReferer() string {
	return self.Request.GetReferer()
}

func (self *Context) SetTemp(key string, value interface{}) *Context {
	self.Request.SetTemp(key, value)
	return self
}

func (self *Context) setUrl(url string) *Context {
	self.Request.Url = url
	return self
}

func (self *Context) SetReferer(referer string) *Context {
	self.Request.Header.Set("Referer", referer)
	return self
}

func (self *Context) UpsertItemField(field string, ruleName ...string) (index int) {
	_, rule, found := self.getRule(ruleName...)
	if !found {
		logs.Log.Error("蜘蛛 %s 调用UpsertItemField()时，指定的规则名不存在！", self.spider.GetName())
		return
	}
	return self.spider.UpsertItemField(rule, field)
}

func (self *Context) Aid(aid map[string]interface{}, ruleName ...string) interface{} {
	self.spider.tryPanic()
	_, rule, found := self.getRule(ruleName...)
	if !found {
		if len(ruleName) > 0 {
			logs.Log.Error("调用蜘蛛 %s 不存在的规则: %s", self.spider.GetName(), ruleName[0])
		} else {
			logs.Log.Error("调用蜘蛛 %s 的Aid()时未指定的规则名", self.spider.GetName())
		}
		return nil
	}
	if rule.AidFunc == nil {
		logs.Log.Error("蜘蛛 %s 的规则 %s 未定义AidFunc", self.spider.GetName(), ruleName[0])
		return nil
	}
	return rule.AidFunc(self, aid)
}

func (self *Context) Parse(ruleName ...string) *Context {
	self.spider.tryPanic()
	_ruleName, rule, found := self.getRule(ruleName...)
	if self.Response != nil {
		self.Request.SetRuleName(_ruleName)
	}
	if !found {
		self.spider.RuleTree.Root(self)
		return self
	}
	if rule.ParseFunc == nil {
		logs.Log.Error("蜘蛛 %s 的规则 %s 未定义ParseFunc", self.spider.GetName(), ruleName[0])
		return self
	}
	rule.ParseFunc(self)
	return self
}

func (self *Context) SetKeyin(keyin string) *Context {
	self.spider.SetKeyin(keyin)
	return self
}

func (self *Context) SetTimer(id string, tol time.Duration, bell *Bell) bool {
	return self.spider.SetTimer(id, tol, bell)
}

func (self *Context) RunTimer(id string) bool {
	return self.spider.RunTimer(id)
}

func (self *Context) ResetText(body string) *Context {
	x := (*[2]uintptr)(unsafe.Pointer(&body))
	h := [3]uintptr{x[0], x[1], x[1]}
	self.text = *(*[]byte)(unsafe.Pointer(&h))
	self.dom = nil
	return self
}

func (self *Context) GetError() error {
	self.spider.tryPanic()
	return self.err
}

func (*Context) Log() logs.Logs {
	return logs.Log
}

func (self *Context) GetSpider() *Spider {
	return self.spider
}

func (self *Context) GetResponse() *http.Response {
	return self.Response
}

func (self *Context) GetStatusCode() int {
	return self.Response.StatusCode
}

func (self *Context) GetRequest() *request.Request {
	return self.Request
}

func (self *Context) CopyRequest() *request.Request {
	return self.Request.Copy()
}

func (self *Context) GetItemFields(ruleName ...string) []string {
	_, rule, found := self.getRule(ruleName...)
	if !found {
		logs.Log.Error("蜘蛛 %s 调用GetItemField（)时，指定的规则名不存在！", self.spider.GetName())
		return nil
	}
	return self.spider.GetItemFields(rule)
}

func (self *Context) GetItemField(index int, ruleName ...string) (field string) {
	_, rule, found := self.getRule(ruleName...)
	if !found {
		logs.Log.Error("蜘蛛 %s 调用GetItemField()时，指定的规则名不存在！", self.spider.GetName())
		return
	}
	return self.spider.GetItemField(rule, index)
}

func (self *Context) GetItemFieldIndex(field string, ruleName ...string) (index int) {
	_, rule, found := self.getRule(ruleName...)
	if !found {
		logs.Log.Error("蜘蛛 %s 调用GetItemField()时，指定的规则名不存在！", self.spider.GetName())
		return
	}
	return self.spider.GetItemFieldIndex(rule, field)
}

func (self *Context) PullItems() (ds []data.DataCell) {
	self.Lock()
	ds = self.items
	self.items = []data.DataCell{}
	self.Unlock()
	return
}

func (self *Context) GetKeyin() string {
	return self.spider.GetKeyin()
}

func (self *Context) GetLimit() int {
	return int(self.spider.GetLimit())
}

func (self *Context) GetName() string {
	return self.spider.GetName()
}

func (self *Context) GetRule(ruleName string) (*Rule, bool) {
	return self.spider.GetRule(ruleName)
}

func (self *Context) GetTemp(key string, defaultValue interface{}) interface{} {
	return self.Request.GetTemp(key, defaultValue)
}

func (self *Context) GetTemps() request.Temp {
	return self.Request.GetTemps()
}

func (self *Context) CopyTemps() request.Temp {
	temps := make(request.Temp)
	for k, v := range self.Request.GetTemps() {
		temps[k] = v
	}
	return temps
}

func (self *Context) GetMethod() string {
	return self.Request.GetMethod()
}

func (self *Context) GetHost() string {
	return self.Response.Request.URL.Host
}

func (self *Context) GetHeader() http.Header {
	return self.Response.Header
}

func (self *Context) GetRequestHeader() http.Header {
	return self.Response.Request.Header
}

func (self *Context) GetCookie() string {
	return self.Response.Header.Get("Set-Cookie")
}

func (self *Context) GetDom() *goquery.Document {
	if self.dom == nil {
		self.initDom()
	}
	return self.dom
}

func (self *Context) GetText() string {
	if self.text == nil {
		self.initText()
	}
	return util.Bytes2String(self.text)
}

func (self *Context) initDom() *goquery.Document {
	if self.text == nil {
		self.initText()
	}
	var err error
	self.dom, err = goquery.NewDocumentFromReader(bytes.NewReader(self.text))
	if err != nil {
		panic(err.Error())
	}
	return self.dom
}

func (self *Context) initText() {
	var err error
	if self.Request.DownloaderID == request.SURF_ID {
		var contentType, pageEncode string
		contentType = self.Response.Header.Get("Content-Type")
		if _, params, err := mime.ParseMediaType(contentType); err == nil {
			if cs, ok := params["charset"]; ok {
				pageEncode = strings.ToLower(strings.TrimSpace(cs))
			}
		}
		if len(pageEncode) == 0 {
			contentType = self.Response.Header.Get("Content-Type")
			if _, params, err := mime.ParseMediaType(contentType); err == nil {
				if cs, ok := params["charset"]; ok {
					pageEncode = strings.ToLower(strings.TrimSpace(cs))
				}
			}
		}

		switch pageEncode {
		case "utf8", "utf-8", "unicode-1-1-utf-8":
		default:
			var destReader io.Reader
			if len(pageEncode) == 0 {
				destReader, err = charset.NewReader(self.Response.Body, "")
			} else {
				destReader, err = charset.NewReaderLabel(pageEncode, self.Response.Body)
			}

			if err == nil {
				self.text, err = ioutil.ReadAll(destReader)
				if err == nil {
					self.Response.Body.Close()
					return
				} else {
					logs.Log.Warning(" *     [convert][%v]: %v (ignore transcoding)\n", self.GetUrl(), err)
				}
			} else {
				logs.Log.Warning(" *     [convert][%v]: %v (ignore transcoding)\n", self.GetUrl(), err)
			}
		}
	}

	self.text, err = ioutil.ReadAll(self.Response.Body)
	self.Response.Body.Close()
	if err != nil {
		panic(err.Error())
		return
	}
}