package surfer

import (
	// "io"
	// "io/ioutil"
	"log"
	// "net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	// "golang.org/x/net/html/charset"
)

func UrlEncode(urlStr string) (*url.URL, error) {
	urlObj, err := url.Parse(urlStr)
	//url.Query() 方法解析RawQuery字段并返回其表示的Values类型键值对
	urlObj.RawQuery = urlObj.Query().Encode()
	return urlObj, err
}

func WalkDir(targpaths string, suffixes ...string) (dirlist []string) {
	if !filepath.IsAbs(targpaths) {
		targpaths, _ = filepath.Abs(targpaths)
	}
	err := filepath.Walk(targpaths, func(retpath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !f.IsDir() {
			return nil
		}
		if len(suffixes) == 0 {
			dirlist = append(dirlist, retpath)
			return nil
		}
		for _, suffix := range suffixes {
			if strings.HasSuffix(retpath, suffix) {
				dirlist = append(dirlist, retpath)
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("utils.WalkDir: %v", err)
		return
	}
	return
}
