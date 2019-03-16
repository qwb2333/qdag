package node

import (
	"qdag/context"
	"qdag/operator"
	"sync"
)

const (
	Run = iota
	Skip
)

type NodeBaseInterface interface {
	DealMulti(contexts *context.Contexts)
	DealOne(context *context.Context)
	Filter(context *context.Context) int
	Init(config map[string]interface{}, args map[string]interface{})
	SetId(id int)
	CreateData(config map[string]interface{}, args map[string]interface{}) interface{}
	GetData() interface{}
	GetName() string
	GetId() int

	IsMultiMode() bool
	SetMultiMode(multiMode bool)
	GetNextNodes() []NodeBaseInterface
	GetPreNodes() []NodeBaseInterface
	Connect(nd NodeBaseInterface)
	AddNextNode(nd NodeBaseInterface)
	AddPreNode(nd NodeBaseInterface)
}

type NodeBase struct {
	id           int
	name         string

	multiMode  bool
	nextNodes    []NodeBaseInterface
	preNodes     []NodeBaseInterface
	operatorData interface{}
	mutex        sync.RWMutex
}

func (this *NodeBase) IsMultiMode() bool {
	return this.multiMode
}

func (this *NodeBase) SetMultiMode(multiMode bool) {
	this.multiMode = multiMode
}

func (this *NodeBase) SetId(id int) {
	this.id = id
}

func (this *NodeBase) AddNextNode(nd NodeBaseInterface) {
	this.nextNodes = append(this.nextNodes, nd)
}

func (this *NodeBase) AddPreNode(nd NodeBaseInterface) {
	this.preNodes = append(this.preNodes, nd)
}

func (this *NodeBase) Connect(nd NodeBaseInterface) {
	this.AddNextNode(nd)
	nd.AddPreNode(this)
}

func (this *NodeBase) GetId() int {
	return this.id
}

func (this *NodeBase) GetName() string {
	return this.name
}

func (this *NodeBase) GetNextNodes() []NodeBaseInterface {
	return this.nextNodes
}

func (this *NodeBase) GetPreNodes() []NodeBaseInterface {
	return this.preNodes
}

func (this *NodeBase) DealMulti(contexts *context.Contexts) {

}

func (this *NodeBase) DealOne(context *context.Context) {

}

func (this *NodeBase) Filter(context *context.Context) int {
	return Run
}

func (this *NodeBase) Init(config map[string]interface{}, args map[string]interface{}) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.operatorData = this.CreateData(config, args)
	dataBase := this.operatorData.(*operator.DataBase)
	this.name = dataBase.Args["name"].(string)
}

func (this *NodeBase) CreateData(config map[string]interface{}, args map[string]interface{}) interface{} {
	return &operator.DataBase{config, args}
}

func (this *NodeBase) GetData() interface{} {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.operatorData
}
