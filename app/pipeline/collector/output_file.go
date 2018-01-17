package collector

import (
	"bytes"
	bytesSize "github.com/henrylee2cn/pholcus/common/bytes"
	"github.com/henrylee2cn/pholcus/common/util"
	"github.com/henrylee2cn/pholcus/config"
	"github.com/henrylee2cn/pholcus/logs"
	"github.com/l-dandelion/gospider/app/pipeline/collector/data"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

func (self *Collector) outputFile(file data.FileCell) {
	defer func() {
		data.PutFileCell(file)
		self.wait.Done()
	}()

	p, n := filepath.Split(filepath.Clean(file["Name"].(string)))
	dir := filepath.Join(config.FILE_DIR, util.FileNameReplace(self.namespace()), p)

	fileName := filepath.Join(dir, util.FileNameReplace(n))

	d, err := os.Stat(dir)
	if err != nil || !d.IsDir() {
		if err := os.MkdirAll(dir, 0777); err != nil {
			logs.Log.Error(" *     Fail  [文件下载：%v | KEYIN：%v | 批次：%v]   %v [ERROR]  %v\n",
				self.Spider.GetName(), self.Spider.GetKeyin(), atomic.LoadUint64(&self.fileBatch), fileName, err)
			return
		}
	}

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		logs.Log.Error(
			" *     Fail  [文件下载：%v | KEYIN：%v | 批次：%v]   %v [ERROR]  %v\n",
			self.Spider.GetName(), self.Spider.GetKeyin(), atomic.LoadUint64(&self.fileBatch), fileName, err,
		)
		return
	}

	size, err := io.Copy(f, bytes.NewReader(file["Bytes"].([]byte)))
	f.Close()
	if err != nil {
		logs.Log.Error(
			" *     Fail  [文件下载：%v | KEYIN：%v | 批次：%v]   %v (%s) [ERROR]  %v\n",
			self.Spider.GetName(), self.Spider.GetKeyin(), atomic.LoadUint64(&self.fileBatch), fileName, bytesSize.Format(uint64(size)), err,
		)
		return
	}
	self.addFileSum(1)

	logs.Log.Informational(" * ")
	logs.Log.App(
		" *     [文件下载：%v | KEYIN：%v | 批次：%v]   %v (%s)\n",
		self.Spider.GetName(), self.Spider.GetKeyin(), atomic.LoadUint64(&self.fileBatch), fileName, bytesSize.Format(uint64(size)),
	)
	logs.Log.Informational(" * ")
}
