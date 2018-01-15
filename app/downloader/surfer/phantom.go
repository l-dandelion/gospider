package surfer

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type (
	Phantom struct {
		PhantomjsFile string
		TempJsDir     string
		jsFileMap     map[string]string
	}
	Respone struct {
		Cookies []string
		Body    string
	}
)

func NewPhantom(phantomjsFile, tempJsDir string) Surfer {
	phantom := &Phantom{
		PhantomjsFile: phantomjsFile,
		TempJsDir:     tempJsDir,
		jsFileMap:     make(map[string]string),
	}

	if !filepath.IsAbs(phantom.PhantomjsFile) {
		phantom.PhantomjsFile, _ = filepath.Abs(phantom.PhantomjsFile)
	}
	if !filepath.IsAbs(phantom.TempJsDir) {
		phantom.TempJsDir, _ = filepath.Abs(phantom.TempJsDir)
	}

	err := os.MkdirAll(phantom.TempJsDir, 0777)
	if err != nil {
		log.Printf("[E] Surfer: %v\n", err)
		return phantom
	}
	phantom.createJsFile("js", js)
	return phantom
}

func (self *Phantom) Download(req Request) (resp *http.Response, err error) {
	var encoding = "utf-8"
	if _, params, err := mime.ParseMediaType(req.GetHeader().Get("Content-Type")); err == nil {
		if cs, ok := params["charset"]; ok {
			encoding = strings.ToLower(strings.TrimSpace(cs))
		}
	}

	req.GetHeader().Del("Content-Type")

	param, err := NewParam(req)
	if err != nil {
		return nil, err
	}
	resp = param.writeback(resp)

	var args = []string{
		self.jsFileMap["js"],
		req.GetUrl(),
		param.header.Get("Cookie"),
		encoding,
		param.header.Get("User-Agent"),
		req.GetPostData(),
		strings.ToLower(param.method),
	}

	for i := 0; i < param.tryTimes; i++ {
		cmd := exec.Command(self.PhantomjsFile, args...)
		b, err := cmd.CombinedOutput()
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		if err != nil || len(b) == 0 {
			time.Sleep(param.retryPause)
			continue
		}
		retResp := Respone{}
		err = json.Unmarshal(b, &retResp)
		if err != nil {
			time.Sleep(param.retryPause)
			continue
		}
		resp.Header = param.header
		for _, cookie := range retResp.Cookies {
			resp.Header.Add("Set-Cookie", cookie)
		}
		resp.Body = ioutil.NopCloser(strings.NewReader(retResp.Body))
		break
	}
	if err == nil {
		resp.StatusCode = http.StatusOK
		resp.Status = http.StatusText(http.StatusOK)
	} else {
		resp.StatusCode = http.StatusBadGateway
		resp.Status = http.StatusText(http.StatusBadGateway)
	}
	return
}

func (self *Phantom) DestroyJsFiles() {
	p, _ := filepath.Split(self.TempJsDir)
	if p == "" {
		return
	}
	for _, filename := range self.jsFileMap {
		os.Remove(filename)
	}
	if len(WalkDir(p)) == 1 {
		os.Remove(p)
	}
}

func (self *Phantom) createJsFile(fileName, jsCode string) {
	fullFileName := filepath.Join(self.TempJsDir, fileName)
	f, _ := os.Create(fullFileName)
	f.Write([]byte(jsCode))
	f.Close()
	self.jsFileMap[fileName] = fullFileName
}

/*
* system.args[0] == post.js
* system.args[1] == url
* system.args[2] == cookie
* system.args[3] == pageEncode
* system.args[4] == userAgent
* system.args[5] == postdata
* system.args[6] == method
 */
const js string = `
var system = require('system');
var page = require('webpage').create();
var url = system.args[1];
var cookie = system.args[2];
var pageEncode = system.args[3];
var userAgent = system.args[4];
var postdata = system.args[5];
var method = system.args[6];
page.onResourceRequested = function(requestData, request) {
    request.setHeader('Cookie', cookie)
};
phantom.outputEncoding = pageEncode;
page.settings.userAgent = userAgent;
page.open(url, method, postdata, function(status) {
   if (status !== 'success') {
        console.log('Unable to access network');
    } else {
        var cookies = new Array();
        for(var i in page.cookies) {
            var cookie = page.cookies[i];
            var c = cookie["name"] + "=" + cookie["value"];
            for (var obj in cookie){
                if(obj == 'name' || obj == 'value'){
                    continue;
                }
                c +=  "; " +　obj + "=" +  cookie[obj];
            }
            cookies[i] = c;
        }
        var resp = {
            "Cookies": cookies,
            "Body": page.content
        };
        console.log(JSON.stringify(resp));
    }
    phantom.exit();
});
`
