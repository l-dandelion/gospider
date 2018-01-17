package pipeline

import (
	"github.com/l-dandelion/gospider/app/pipeline/collector"
	"github.com/l-dandelion/gospider/app/pipeline/collector/data"
	"github.com/l-dandelion/gospider/app/spider"
)

type Pipeline interface {
	Start()
	Stop()
	CollectData(data.DataCell) error
	CollectFile(data.FileCell) error
}

func New(sp *spider.Spider) Pipeline {
	return collector.NewCollector(sp)
}
