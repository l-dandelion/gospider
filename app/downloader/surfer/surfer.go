package surfer

import (
	"net/http"
	"sync"
)

type Surfer interface {
	Download(Request) (resp *http.Response, err error)
}

var (
	surf          Surfer
	phantom       Surfer
	once_surf     sync.Once
	once_phantom  sync.Once
	tempJsDir     = "./tmp"
	phantomjsFile = `./phantomjs`
)

func Download(req Request) (resp *http.Response, err error) {
	switch req.GetDownloaderID() {
	case SurfID:
		once_surf.Do(func() {
			surf = New()
		})
		resp, err = surf.Download(req)
	case PhomtomJsID:
		once_phantom.Do(func() {
			phantom = NewPhantom(phantomjsFile, tempJsDir)
		})
		resp, err = phantom.Download(req)
	}
	return
}

func DestroyJsFiles() {
	if pt, ok := phantom.(*Phantom); ok {
		pt.DestroyJsFiles()
	}
}
