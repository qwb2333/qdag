package executor

import (
	"log"
	"qdag/context"
	"qdag/operator/node"
	"qdag/operator/source"
	"qdag/util"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerQueueShareData struct {
	mutex         sync.Mutex
	contextsArray []*context.Contexts
	doneCount     []int
	remainCount   int
}

func NewWorkerQueueShareData(src source.SourceBaseInterface) *WorkerQueueShareData {
	ret := &WorkerQueueShareData{}
	totalNodesSize := src.GetTotalNodesSize()
	ret.contextsArray = make([]*context.Contexts, totalNodesSize)
	ret.doneCount = make([]int, totalNodesSize)
	ret.remainCount = totalNodesSize
	return ret
}

type WorkerQueueData struct {
	node      node.NodeBaseInterface
	shareData *WorkerQueueShareData
}

func (this WorkerQueueData) GetContexts() *context.Contexts {
	return this.shareData.contextsArray[this.node.GetId()]
}

type DAGExecutor struct {
	runable                 int32
	runningKeyMap           sync.Map
	config                  map[string]interface{}
	contextQueueMap         map[string]chan *context.Context
	lowPriorityWorkerQueue  chan *WorkerQueueData
	highPriorityWorkerQueue chan *WorkerQueueData
	stopSignal              chan bool
}

func NewDAGExecutor(config map[string]interface{}) *DAGExecutor {
	ret := &DAGExecutor{}
	ret.runable = 1
	ret.config = config
	return ret
}

func (this *DAGExecutor) runSource(src source.SourceBaseInterface) {
	maxDupKeyContextsSize := this.config["max_dup_key_contexts_size"].(int)
	sourceName := src.GetName()
	sourceId := src.GetId()
	log.Printf("Source(name=%s, id=%d) run\n", sourceName, sourceId)

	addContextFunc := func(ctx *context.Context) {
		this.contextQueueMap[sourceName]<-ctx
	}

	dupKeyContexts := make([]*context.Context, 0)
	dealDupKeyContextsFunc := func() {
		newIndex := 0
		dupKeyLen := len(dupKeyContexts)
		for i := 0; i < dupKeyLen; i++ {
			ctx := dupKeyContexts[i]
			if this.TryAddRunningKey(ctx.GetKey()) {
				log.Printf("dupKey wait end, key=%v\n", ctx.GetKey())
				addContextFunc(ctx)
			} else {
				dupKeyContexts[newIndex] = ctx
				newIndex++
			}
		}
		dupKeyContexts = dupKeyContexts[:newIndex]
	}

	for atomic.LoadInt32(&this.runable) > 0 {
		dealDupKeyContextsFunc()

		contexts := src.Fetch()
		for _, ctx := range contexts.GetContexts() {
			if this.TryAddRunningKey(ctx.GetKey()) {
				addContextFunc(ctx)
			} else {
				var retryCount uint
				log.Printf("dupKey, key=%v, len(dupKeyContexts)=%d\n", ctx.GetKey(), len(dupKeyContexts))
				for retryCount = 0; len(dupKeyContexts) >= maxDupKeyContextsSize; retryCount++ {
					sleepTime := 1 << retryCount
					log.Printf("dupKeyContext over limit, retryCount=%d, sleep for %ds\n", retryCount, sleepTime)
					time.Sleep(time.Second * time.Duration(sleepTime))
					dealDupKeyContextsFunc()
				}

				if retryCount > 0 && this.TryAddRunningKey(ctx.GetKey()) {
					addContextFunc(ctx)
				} else {
					dupKeyContexts = append(dupKeyContexts, ctx)
				}
			}
		}
	}
}

func (this *DAGExecutor) runWorker(id int) {
	log.Printf("Worker(%d) run", id)
	var workerData *WorkerQueueData = nil
	for atomic.LoadInt32(&this.runable) > 0 {

		if workerData == nil {
			select {
			case workerData = <- this.highPriorityWorkerQueue:
			default:
				select {
				case workerData = <- this.highPriorityWorkerQueue:
				case workerData = <- this.lowPriorityWorkerQueue:
				}
			}
		}

		nd := workerData.node
		contexts := workerData.GetContexts()
		size := contexts.Size()
		if nd.IsMultiMode() {
			validIds := make([]int, 0)
			for i := 0; i < size; i++ {
				result := nd.Filter(contexts.GetContextById(i))
				if result == node.Run {
					validIds = append(validIds, i)
				}
			}
			if 0 < len(validIds) && len(validIds) < size {
				newContexts := context.NewContexts()
				for validId := range validIds {
					newContexts.Add(contexts.GetContextById(validId))
				}
				nd.DealMulti(newContexts)
			} else if len(validIds) == size {
				nd.DealMulti(workerData.GetContexts())
				for _, ctx := range contexts.GetContexts() {
					nd.DealOne(ctx)
				}
			}
		} else {
			for _, ctx := range contexts.GetContexts() {
				result := nd.Filter(ctx)
				if result == node.Run {
					nd.DealOne(ctx)
				}
			}
		}

		finished := false
		shareData := workerData.shareData
		triggerNodes := make([]node.NodeBaseInterface, 0)

		shareData.mutex.Lock()
		shareData.remainCount--
		for _, nextNode := range nd.GetNextNodes() {
			nextNodeId := nextNode.GetId()
			shareData.doneCount[nextNodeId]++
			if shareData.doneCount[nextNodeId] == len(nextNode.GetPreNodes()) {
				triggerNodes = append(triggerNodes, nextNode)
			}
		}
		if shareData.remainCount == 0 {
			finished = true
		}
		shareData.mutex.Unlock()

		if len(triggerNodes) > 0 {
			for idx, triggerNode := range triggerNodes {
				preNodes := triggerNode.GetPreNodes()
				if len(preNodes) > 0 {
					// Merge pre Contexts
					shareData.contextsArray[triggerNode.GetId()] = shareData.contextsArray[preNodes[0].GetId()].LightClone()
					for i := 0; i < len(preNodes); i++ {
						shareData.contextsArray[triggerNode.GetId()].Merge(shareData.contextsArray[preNodes[i].GetId()])
					}
				}

				if idx == 0 {
					workerData = &WorkerQueueData{triggerNode, shareData}
				} else {
					this.highPriorityWorkerQueue <- &WorkerQueueData{triggerNode, shareData}
				}
			}
		} else {
			workerData = nil
		}

		if finished {
			now := util.GetTimeStampMs()
			for _, ctx := range contexts.GetContexts() {
				key := ctx.GetKey()
				beginTime, _ := this.runningKeyMap.Load(key)
				this.runningKeyMap.Delete(key)
				log.Printf("key=%v finished, time=%dms\n", key, now - beginTime.(int64))
			}
		}
	}
}

func (this *DAGExecutor) runSourceJoin(sourceName string, src source.SourceBaseInterface) {
	log.Printf("SourceJoin(%s) run", sourceName)
	maxWaitTime := this.config["max_wait_time_ms"].(int)
	maxBatchSize := this.config["max_batch_size"].(int)
	queue := this.contextQueueMap[sourceName]

	for atomic.LoadInt32(&this.runable) > 0 {
		contexts := context.NewContexts()
		for i := 0; i < maxBatchSize; i++ {
			select {
			case ctx := <-queue:
				contexts.Add(ctx)
			default:
				time.Sleep(time.Duration(maxWaitTime) * time.Microsecond)
			}
		}

		if contexts.Size() > 0 {
			log.Printf("SourceJoin(%s) get contexts, len=%d\n", sourceName, contexts.Size())
			shareData := NewWorkerQueueShareData(src)
			nextNodes := src.GetNextNodes()

			for i := 0; i < len(nextNodes); i++ {
				nd := nextNodes[i]
				if i == 0 {
					shareData.contextsArray[nd.GetId()] = contexts
				} else {
					shareData.contextsArray[nd.GetId()] = contexts.LightClone()
				}

				this.lowPriorityWorkerQueue<-&WorkerQueueData{nd, shareData}
			}
		}
	}
}

func (this *DAGExecutor) TryAddRunningKey(key interface{}) bool {
	now := util.GetTimeStampMs()
	_, ok := this.runningKeyMap.LoadOrStore(key, now)
	return !ok
}

func (this *DAGExecutor) Run(sources []source.SourceBaseInterface) {
	maxWorkerQueueSize := this.config["max_worker_queue_size"].(int)
	maxContextQueueSize := this.config["max_context_queue_size"].(int)

	this.contextQueueMap = make(map[string]chan *context.Context)
	this.lowPriorityWorkerQueue = make(chan *WorkerQueueData, maxWorkerQueueSize)
	this.highPriorityWorkerQueue = make(chan *WorkerQueueData, maxWorkerQueueSize)

	sourceNameMap := make(map[string]source.SourceBaseInterface)
	for _, src := range sources {
		sourceName := src.GetName()
		if this.contextQueueMap[sourceName] == nil {
			sourceNameMap[sourceName] = src
			this.contextQueueMap[sourceName] = make(chan *context.Context, maxContextQueueSize)
		}
	}

	for i := 0; i < this.config["worker_num"].(int); i++ {
		go this.runWorker(i)
	}

	for _, src := range sources {
		go this.runSource(src)
	}

	for sourceName, src := range sourceNameMap {
		go this.runSourceJoin(sourceName, src)
	}

	<-this.stopSignal
}

func (this *DAGExecutor) Stop() {
	atomic.StoreInt32(&this.runable, 0)
	this.stopSignal<-true
}
