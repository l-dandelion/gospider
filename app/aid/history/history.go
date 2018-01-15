package history

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"

	"gopkg.in/mgo.v2/bson"

	"github.com/henrylee2cn/pholcus/common/mgo"
	"github.com/henrylee2cn/pholcus/common/mysql"
	"github.com/henrylee2cn/pholcus/common/pool"
	"github.com/henrylee2cn/pholcus/common/util"
	"github.com/henrylee2cn/pholcus/config"
	"github.com/henrylee2cn/pholcus/logs"
	"github.com/l-dandelion/gospider/app/downloader/request"
)

type (
	Historier interface {
		ReadSuccess(provider string, inherit bool) //读取成功记录
		UpsertSuccess(string) bool
		HasSuccess(string) bool
		DeleteSuccess(string)
		FlushSuccess(provider string)

		ReadFailure(provider string, inherit bool)
		PullFailure() map[string]*request.Request
		UpsertFailure(*request.Request) bool
		DeleteFailure(*request.Request)
		FlushFailure(provider string)

		Empty()
	}

	History struct {
		*Success
		*Failure
		provider string
		sync.RWMutex
	}
)

const (
	SUCCESS_SUFFIX = config.HISTORY_TAG + "__y"
	FAILURE_SUFFIX = config.HISTORY_TAG + "__n"
	SUCCESS_FILE   = config.HISTORY_DIR + "/" + SUCCESS_SUFFIX
	FAILURE_FILE   = config.HISTORY_DIR + "/" + FAILURE_SUFFIX
)

func New(name string, subName string) Historier {
	successTabName := SUCCESS_SUFFIX + "__" + name
	successFileName := SUCCESS_FILE + "__" + name
	failureTabName := FAILURE_SUFFIX + "__" + name
	failureFileName := FAILURE_FILE + "__" + name

	if subName != "" {
		successTabName += "__" + subName
		successFileName += "__" + subName
		failureTabName += "__" + subName
		failureFileName += "__" + subName
	}
	return &History{
		Success: &Success{
			tabName:  util.FileNameReplace(successTabName),
			fileName: successFileName,
			new:      make(map[string]bool),
			old:      make(map[string]bool),
		},
		Failure: &Failure{
			tabName:  util.FileNameReplace(failureTabName),
			fileName: failureFileName,
			list:     make(map[string]*request.Request),
		},
	}
}

func (self *History) ReadSuccess(provider string, inherit bool) {
	self.RWMutex.Lock()
	self.provider = provider
	self.RWMutex.Unlock()

	if !inherit {
		self.Success.old = make(map[string]bool)
		self.Success.new = make(map[string]bool)
		self.Success.inheritable = false
	} else if self.Success.inheritable {
		return
	} else {
		self.Success.old = make(map[string]bool)
		self.Success.new = make(map[string]bool)
		self.Success.inheritable = true
	}

	switch provider {
	case "mgo":
		var docs = map[string]interface{}{}
		err := mgo.Mgo(&docs, "find", map[string]interface{}{
			"Database":   config.DB_NAME,
			"Collection": self.Success.tabName,
		})
		if err != nil {
			logs.Log.Error(" * Fail [读取成功记录][mgo]: %v\n", err)
			return
		}
		for _, v := range docs["Docs"].([]interface{}) {
			self.Success.old[v.(bson.M)["_id"].(string)] = true
		}
	case "mysql":
		_, err := mysql.DB()
		if err != nil {
			logs.Log.Error(" *     Fail [读取成功记录][mysql]： %v\n", err)
			return
		}
		table, ok := getReadMysqlTable(self.Success.tabName)
		if !ok {
			table = mysql.New().SetTableName(self.Success.tabName)
			setReadMysqlTable(self.Success.tabName, table)
		}
		rows, err := table.SelectAll()
		if err != nil {
			return
		}
		for rows.Next() {
			var id string
			err = rows.Scan(&id)
			self.Success.old[id] = true
		}
	default:
		f, err := os.Open(self.Success.fileName)
		if err != nil {
			return
		}
		defer f.Close()
		b, _ := ioutil.ReadAll(f)
		if len(b) == 0 {
			return
		}
		b[0] = '{'
		json.Unmarshal(append(b, '}'), &self.Success.old)
	}
	logs.Log.Informational(" *     [读取成功记录]: %v 条\n", len(self.Success.old))
}

func (self *History) ReadFailure(provider string, inherit bool) {
	self.RWMutex.Lock()
	self.provider = provider
	self.RWMutex.Unlock()

	if !inherit {
		self.Failure.list = make(map[string]*request.Request)
		self.Failure.inheritable = false
		return
	} else if self.Failure.inheritable {
		return
	} else {
		self.Failure.list = make(map[string]*request.Request)
		self.Failure.inheritable = true
	}
	var fLen int
	switch provider {
	case "mgo":
		if mgo.Error() != nil {
			logs.Log.Error(" *     Fail [取出失败记录][mgo]: %v\n", mgo.Error())
			return
		}

		var docs = []interface{}{}
		mgo.Call(func(src pool.Src) error {
			c := src.(*mgo.MgoSrc).DB(config.DB_NAME).C(self.Failure.tabName)
			return c.Find(nil).All(&docs)
		})

		fLen = len(docs)

		for _, v := range docs {
			key := v.(bson.M)["_id"].(string)
			failure := v.(bson.M)["failure"].(string)
			req, err := request.UnSerialize(failure)
			if err != nil {
				continue
			}
			self.Failure.list[key] = req
		}
	case "mysql":
		_, err := mysql.DB()
		if err != nil {
			logs.Log.Error(" *     Fail [取出失败记录][mysql]: %v\n", err)
			return
		}
		table, ok := getReadMysqlTable(self.Failure.tabName)
		if !ok {
			table = mysql.New().SetTableName(self.Failure.tabName)
			setReadMysqlTable(self.Failure.tabName, table)
		}
		rows, err := table.SelectAll()
		if err != nil {
			return
		}
		for rows.Next() {
			var key, failure string
			err = rows.Scan(&key, &failure)
			req, err := request.UnSerialize(failure)
			if err != nil {
				continue
			}
			self.Failure.list[key] = req
			fLen++
		}

	default:
		f, err := os.Open(self.Failure.fileName)
		if err != nil {
			return
		}
		b, _ := ioutil.ReadAll(f)
		f.Close()

		if len(b) == 0 {
			return
		}

		docs := map[string]string{}
		json.Unmarshal(b, &docs)

		fLen = len(docs)

		for key, s := range docs {
			req, err := request.UnSerialize(s)
			if err != nil {
				continue
			}
			self.Failure.list[key] = req
		}
	}

	logs.Log.Informational(" *     [取出失败记录]: %v 条\n", fLen)
}

func (self *History) Empty() {
	self.RWMutex.Lock()
	self.Success.new = make(map[string]bool)
	self.Success.old = make(map[string]bool)
	self.Failure.list = make(map[string]*request.Request)
	self.RWMutex.Unlock()
}

func (self *History) FlushSuccess(provider string) {
	self.RWMutex.Lock()
	self.provider = provider
	self.RWMutex.Unlock()
	sucLen, err := self.Success.flush(provider)
	if sucLen <= 0 {
		return
	}
	if err != nil {
		logs.Log.Error("%v", err)
	} else {
		logs.Log.Informational(" *     [输出成功记录]: %v 条\n", sucLen)
	}
}

func (self *History) FlushFailure(provider string) {
	self.RWMutex.Lock()
	self.provider = provider
	self.RWMutex.Unlock()
	failLen, err := self.Failure.flush(provider)
	if failLen <= 0 {
		return
	}
	if err != nil {
		logs.Log.Error("%v", err)
	} else {
		logs.Log.Informational(" *     [输出失败记录]: %v 条\n", failLen)
	}
}

var (
	readMysqlTable     = map[string]*mysql.MyTable{}
	readMysqlTableLock sync.RWMutex
)

func getReadMysqlTable(name string) (*mysql.MyTable, bool) {
	readMysqlTableLock.RLock()
	tab, ok := readMysqlTable[name]
	readMysqlTableLock.RUnlock()
	if ok {
		return tab.Clone(), true
	}
	return nil, false
}

func setReadMysqlTable(name string, tab *mysql.MyTable) {
	readMysqlTableLock.Lock()
	readMysqlTable[name] = tab
	readMysqlTableLock.Unlock()
}

var (
	writeMysqlTable     = map[string]*mysql.MyTable{}
	writeMysqlTableLock sync.RWMutex
)

func getWriteMysqlTable(name string) (*mysql.MyTable, bool) {
	writeMysqlTableLock.RLock()
	tab, ok := writeMysqlTable[name]
	writeMysqlTableLock.RUnlock()
	if ok {
		return tab.Clone(), true
	}
	return nil, false
}

func setWriteMysqlTable(name string, tab *mysql.MyTable) {
	writeMysqlTableLock.Lock()
	writeMysqlTable[name] = tab
	writeMysqlTableLock.Unlock()
}
