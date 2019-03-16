package source

import (
	"qdag/context"
	"qdag/operator"
	"qdag/operator/node"
	"sync"
)

type SourceBaseInterface interface {
	GetId() int
	SetId(id int)
	GetName() string
	Fetch() *context.Contexts
	Init(splitNum int, splitId int, config map[string]interface{}, args map[string]interface{})
	CreateData(splitNum int, splitId int, config map[string]interface{}, args map[string]interface{}) interface{}
	GetData() interface{}
	GetNextNodes() []node.NodeBaseInterface
	GetTotalNodesSize() int
	SetTotalNodesSize(total int)
	IsMultiMode() bool
	Connect(nd node.NodeBaseInterface)
}

type SourceBase struct {
	id             int
	name           string

	splitId        int
	splitNum       int
	operatorData   interface{}
	mutex          sync.RWMutex
	nextNodes      []node.NodeBaseInterface
	totalNodesSize int
	multiMode    bool
}

func (this *SourceBase) Connect(nd node.NodeBaseInterface) {
	this.nextNodes = append(this.nextNodes, nd)
}

func (this *SourceBase) IsMultiMode() bool {
	return this.multiMode
}

func (this *SourceBase) SetId(id int) {
	this.id = id
}

func (this *SourceBase) GetId() int {
	return this.id
}

func (this *SourceBase) GetName() string {
	return this.name
}

func (this *SourceBase) GetNextNodes() []node.NodeBaseInterface {
	return this.nextNodes
}

func (this *SourceBase) GetTotalNodesSize() int {
	return this.totalNodesSize
}

func (this *SourceBase) SetTotalNodesSize(total int) {
	this.totalNodesSize = total
}

func (this *SourceBase) Fetch() *context.Contexts {
	return context.NewContexts()
}

func (this *SourceBase) Init(splitNum int, splitId int, config map[string]interface{}, args map[string]interface{}) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.splitId = splitId
	this.splitNum = splitNum
	this.operatorData = this.CreateData(splitNum, splitId, config, args)
	dataBase := this.operatorData.(*operator.DataBase)
	this.name = dataBase.Args["name"].(string)
}

func (this *SourceBase) CreateData(splitNum int, splitId int, config map[string]interface{}, args map[string]interface{}) interface{} {
	return &operator.DataBase{config, args}
}

func (this *SourceBase) GetData() interface{} {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.operatorData
}
