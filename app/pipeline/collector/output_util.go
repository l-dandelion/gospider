package collector

import (
	"github.com/henrylee2cn/pholcus/logs"
)

func (self *Collector) namespace() string {
	if self.Spider.Namespace == nil {
		if self.Spider.GetSubName() == "" {
			return self.Spider.GetName()
		}
		return self.Spider.GetName() + "__" + self.Spider.GetSubName()
	}
	return self.Spider.Namespace(self.Spider)
}

func (self *Collector) subNamespace(dataCell map[string]interface{}) string {
	if self.Spider.SubNamespace == nil {
		return dataCell["RuleName"].(string)
	}
	defer func() {
		if p := recover(); p != nil {
			logs.Log.Error("subNamespace: %v", p)
		}
	}()
	return self.Spider.SubNamespace(self.Spider, dataCell)
}

func joinNamespaces(namespace, subNamespace string) string {
	if namespace == "" {
		return subNamespace
	} else if subNamespace == "" {
		return namespace
	}
	return namespace + "__" + subNamespace
}
