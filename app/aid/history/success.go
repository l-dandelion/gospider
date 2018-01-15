package history

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/henrylee2cn/pholcus/common/mgo"
	"github.com/henrylee2cn/pholcus/common/mysql"
	"github.com/henrylee2cn/pholcus/config"
)

type Success struct {
	tabName     string
	fileName    string
	new         map[string]bool
	old         map[string]bool
	inheritable bool
	sync.RWMutex
}

func (self *Success) UpsertSuccess(reqUnique string) bool {
	self.RWMutex.Lock()
	defer self.RWMutex.Unlock()

	if self.old[reqUnique] {
		return false
	}

	if self.new[reqUnique] {
		return false
	}

	self.new[reqUnique] = true
	return true
}

func (self *Success) HasSuccess(reqUnique string) bool {
	self.RWMutex.RLock()
	has := self.old[reqUnique] || self.new[reqUnique]
	self.RWMutex.RUnlock()
	return has
}

func (self *Success) DeleteSuccess(reqUnique string) {
	self.RWMutex.Lock()
	delete(self.new, reqUnique)
	self.RWMutex.Unlock()
}

func (self *Success) flush(provider string) (sLen int, err error) {
	self.RWMutex.Lock()
	defer self.RWMutex.Unlock()

	sLen = len(self.new)
	if sLen == 0 {
		return
	}

	switch provider {
	case "mgo":
		if mgo.Error() != nil {
			err = fmt.Errorf(" *     Fail [添加成功记录]【mgo]: %v 条 [ERROR] %v\n", sLen, mgo.Error())
			return
		}
		var docs = make([]map[string]interface{}, sLen)
		var i int
		for key := range self.new {
			docs[i] = map[string]interface{}{"_id": key}
			self.old[key] = true
			i++
		}
		err = mgo.Mgo(nil, "insert", map[string]interface{}{
			"Database":    config.DB_NAME,
			"Collecstion": self.tabName,
			"Docs":        docs,
		})
		if err != nil {
			err = fmt.Errorf(" *     Fail [添加成功记录][mgo]: %v 条 [ERROR] %v\n", sLen, err)
		}
	case "mysql":
		_, err := mysql.DB()
		if err != nil {
			return sLen, fmt.Errorf(" *     Fail [添加成功记录][mysql]: %v 条 [ERROR] %v\n", sLen, err)
		}
		table, ok := getWriteMysqlTable(self.tabName)
		if !ok {
			table = mysql.New()
			table.SetTableName(self.tabName).CustomPrimaryKey(`id VARCHAR(255) NOT NULL PRIMARY KEY`)
			err = table.Create()
			if err != nil {
				return sLen, fmt.Errorf(" *     Fail [添加成功记录][mysql]: %v 条[ERROR] %v\n", sLen, err)
			}
			setWriteMysqlTable(self.tabName, table)
		}
		for key := range self.new {
			table.AutoInsert([]string{key})
			self.old[key] = true
		}
		err = table.FlushInsert()
		if err != nil {
			return sLen, fmt.Errorf(" *     Fail [添加成功记录][mysql]: %v 条 [ERROR] %v\n", sLen, err)
		}
	default:
		f, _ := os.OpenFile(self.fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0777)

		b, _ := json.Marshal(self.new)

		b[0] = ','
		f.Write(b[:len(b)-1])
		f.Close()

		for key := range self.new {
			self.old[key] = true
		}
	}
	self.new = make(map[string]bool)
	return
}
