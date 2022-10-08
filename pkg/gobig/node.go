package gobig

import (
	"sync"
)

const (
	SignalForeach  = 1
	SignalReadFile = 2
)

type NodeReturnValue struct {
	Err    error
	Value  map[string]interface{}
	NodeID int
}

type Node struct {
	signalBus  chan ExecutionStep
	returnBus  chan *NodeReturnValue
	isBusy     bool
	mut        *sync.RWMutex
	ID         int
	Partitions []*Partition
}

func NewNode(id int, partitions []*Partition) *Node {
	w := &Node{
		signalBus:  make(chan ExecutionStep),
		isBusy:     false,
		mut:        &sync.RWMutex{},
		ID:         id,
		Partitions: partitions,
	}
	go w.handleExecutionSteps()
	return w
}

func (n *Node) SendExecution(s ExecutionStep, returnBus chan *NodeReturnValue) {
	n.mut.Lock()
	defer n.mut.Unlock()
	//fmt.Printf("received step(%s) at node(%d)\n", s.String(), n.ID)

	n.returnBus = returnBus

	n.isBusy = true
	n.signalBus <- s
}

func (n *Node) pushReturn(ret *NodeReturnValue) {
	if ret == nil {
		ret = &NodeReturnValue{}
	}
	ret.NodeID = n.ID
	n.returnBus <- ret
}

func (n *Node) handleExecutionSteps() {
	for step := range n.signalBus {
		n.mut.Lock()

		// This node received an execution step
		ret := step.Execute(n)
		n.pushReturn(ret)
		n.isBusy = false

		//switch step.Code {
		//case SignalForeach:
		//	partition := step.Metadata["partition"].(*Partition)
		//	fn := step.Metadata["fn"].(func(r *Row))
		//	n.foreach(partition, fn)
		//	n.isBusy = false
		//
		//case SignalReadFile:
		//	filePath := step.Metadata["filePath"].(string)
		//	log.Printf("node(%d) reading file %s\n", n.ID, filePath)
		//	data, err := os.ReadFile(filePath)
		//	if err != nil {
		//		log.Printf("unable to read %s : %v\n", filePath, err)
		//		n.pushReturn(&NodeReturnValue{Err: err})
		//		n.isBusy = false
		//		continue
		//	}
		//
		//	n.pushReturn(&NodeReturnValue{Value: map[string]interface{}{
		//		"data":     data,
		//		"filePath": filePath,
		//	}})
		//	n.isBusy = false
		//
		//default:
		//	log.Panicf("received unrecognized code: %d", step.Code)
		//}

		n.mut.Unlock()
	}
}

func (n *Node) IsBusy() bool {
	n.mut.RLock()
	defer n.mut.RUnlock()

	return n.isBusy
}
