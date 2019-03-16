package context

type Context struct {
	key        interface{}
	readOnly   map[string]interface{}
	writeOnly  map[string]interface{}
}

func NewContext(key interface{}) *Context {
	return &Context {
		key,
		map[string]interface{}{},
		map[string]interface{}{},
	}
}

func (this *Context) GetKey() interface{} {
	return this.key
}

func (this *Context) GetOrNil(key string) interface{} {
	ret := this.writeOnly[key]
	if ret == nil {
		ret = this.readOnly[key]
	}
	return ret
}

func (this *Context) Get(key string, defaultValue interface{}) interface{} {
	ret := this.writeOnly[key]
	if ret == nil {
		ret = this.readOnly[key]
	}

	if ret == nil {
		return defaultValue
	}
	return ret
}

func (this *Context) GetDataKeys() []string {
	ret := make([]string, 0)
	for key, _ := range this.readOnly {
		ret = append(ret, key)
	}
	for key, _ := range this.writeOnly {
		if this.readOnly[key] != nil {
			ret = append(ret, key)
		}
	}
	return ret
}

func (this *Context) LightClone() *Context {
	return &Context{this.key, this.readOnly, map[string]interface{}{}}
}

func (this *Context) Set(key string, value interface{}) {
	this.writeOnly[key] = value
}

func (this *Context) Merge(context *Context) {
	for key, value := range context.writeOnly {
		this.readOnly[key] = value
	}
}

type Contexts struct {
	keys []interface{}
	contexts []*Context
	contextMap map[interface{}]*Context
}

func NewContexts() *Contexts {
	ret := &Contexts{}
	ret.contextMap = make(map[interface{}]*Context)
	return ret
}

func (this *Contexts) GetKeys() []interface{} {
	return this.keys
}

func (this *Contexts) GetContexts() []*Context {
	return this.contexts
}

func (this *Contexts) GetContextByKey(key interface{}) *Context {
	return this.contextMap[key]
}

func (this *Contexts) GetKeyById(id int) interface{} {
	return this.keys[id]
}

func (this *Contexts) GetContextById(id int) *Context {
	return this.contexts[id]
}

func (this *Contexts) Add(context *Context) {
	key := context.GetKey()
	this.keys = append(this.keys, key)
	this.contexts = append(this.contexts, context)
	this.contextMap[key] = context
}

func (this *Contexts) Merge(contexts *Contexts) {
	size := this.Size()
	for i := 0; i < size; i++ {
		this.contexts[i].Merge(contexts.contexts[i])
	}
}

func (this *Contexts) Size() int {
	return len(this.keys)
}

func (this *Contexts) LightClone() *Contexts {
	ret := &Contexts{}
	ret.contextMap = make(map[interface{}]*Context)
	for _, ctx := range this.GetContexts() {
		ret.Add(ctx.LightClone())
	}
	return ret
}
