package tests

import (
	"fmt"
	"qdag/util"
	"testing"
)

type Content struct {
	x []int
	y int
}

func TestArray(testing *testing.T) {
	ctt := &Content{}
	ctt.y = 123

	w := ctt
	w.y = 666
	fmt.Println(ctt.y)
}

func ModifyMap(t map[int]int) {
	t[2333] = 666
}

func TestRefMap(testing *testing.T) {
	t := make(map[int]int)
	t[2333] = 4
	ModifyMap(t)
	fmt.Println(t[2333])
}

type Node struct {
	a int
}

func ModifyNode(t *Node) {
	t.a = 666
}

func TestRefStruct(testing *testing.T) {
	t := Node{1}
	fmt.Println(t.a)
	ModifyNode(&t)
	fmt.Println(t.a)
}

func TestTimeStamp(testing *testing.T) {
	fmt.Println(util.GetTimeStampMs())
}

type AInterface interface {
	PrintWraper()
	print()
}

type BaseA struct {
	x int
}

func (this *BaseA) PrintWraper() {
	this.print()
}

func (this *BaseA) print() {
	fmt.Println("BaseA")
}

type DataA struct {
	BaseA
}

func (this *DataA) print() {
	fmt.Println("DataA")
}

func TestInterface(testing *testing.T) {
	a := AInterface(&DataA{})
	a.PrintWraper()
}

