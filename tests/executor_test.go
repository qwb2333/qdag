package tests

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"qdag/context"
	"qdag/executor"
	"qdag/operator/node"
	"qdag/operator/source"
	"qdag/util"
	"testing"
	"time"
)

type SourceA struct {
	source.SourceBase
}

type NodeA struct {
	node.NodeBase
}

func (this *NodeA) DealOne(ctx *context.Context) {
	rsp, err := http.Get(ctx.Get("url", "http://www.baidu.com").(string))
	if err == nil {
		body, err := ioutil.ReadAll(rsp.Body)
		if err == nil {
			ctx.Set("A", len(body))
		}
	}
}

type NodeB struct {
	node.NodeBase
}

func (this *NodeB) DealOne(ctx *context.Context) {
	ctx.Set("B", 4)
}

type NodeC struct {
	node.NodeBase
}

func (this *NodeC) DealOne(ctx *context.Context) {
	answer := ctx.Get("init", 0).(int) + ctx.Get("A", 0).(int) + ctx.Get("B", 0).(int)
	fmt.Printf("%d,%s,%d\n", ctx.GetKey(), ctx.Get("url", "").(string), answer)
}


func (this *SourceA) Fetch() *context.Contexts {
	urlArr := []string{
		"https://www.baidu.com",
		"https://www.baidu.com",
		"https://blog.csdn.net/liuxuejiang158/column/info/ccia",
		"https://cloud.tencent.com/",
		"https://www.jianshu.com/p/2d429f6efbfc",
		"https://people.bytedance.net/",
		"https://www.toutiao.com/",
		"http://www.365yg.com/a6668605887260656132/#mid=5757425042",
		"https://www.sogou.com/",
		"https://cn.bing.com/academic/profile?id=bd3d6bb97f4e1dadfcab420863a014ec&encoded=0&v=paper_preview&mkt=zh-cn",
	}
	ret := context.NewContexts()
	for i := 0; i < 100; i++ {
		key := rand.Int31()
		ctx := context.NewContext(key)
		ctx.Set("init", 1)
		ctx.Set("url", urlArr[i % 10])
		ret.Add(ctx)
	}
	time.Sleep(time.Millisecond * 1000)
	return ret
}

func makeConfig() map[string]interface{} {
	config := make(map[string]interface{})
	config["worker_num"] = 10000
	config["max_worker_queue_size"] = 1024
	config["max_context_queue_size"] = 1024
	config["max_wait_time_ms"] = 100
	config["max_dup_key_contexts_size"] = 3
	config["max_batch_size"] = 1
	return config
}

func TestSimple(t *testing.T) {
	util.LogInit()
	config := makeConfig()
	sourceA := source.SourceBaseInterface(&SourceA{})
	sourceA.Init(1, 0, config, map[string]interface{}{"name": "sourceA"})

	nodeA := node.NodeBaseInterface(&NodeA{})
	nodeA.Init(config, map[string]interface{}{"name": "nodeA"})

	nodeB := node.NodeBaseInterface(&NodeB{})
	nodeB.Init(config, map[string]interface{}{"name": "nodeB"})

	nodeC := node.NodeBaseInterface(&NodeC{})
	nodeC.Init(config, map[string]interface{}{"name": "nodeC"})

	sourceA.SetId(0)
	sourceA.SetTotalNodesSize(3)
	nodeA.SetId(0)
	nodeB.SetId(1)
	nodeC.SetId(2)

	sourceA.Connect(nodeA)
	sourceA.Connect(nodeB)
	nodeA.Connect(nodeC)
	nodeB.Connect(nodeC)

	exe := executor.NewDAGExecutor(config)
	exe.Run([]source.SourceBaseInterface{sourceA})
}