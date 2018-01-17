package downloader

import (
	"github.com/l-dandelion/gospider/app/downloader/request"
	"github.com/l-dandelion/gospider/app/spider"
)

type Downloader interface {
	Downloader(*spider.Spider, *request.Request) *spider.Context
}
