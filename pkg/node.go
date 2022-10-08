package pkg

import (
	"fmt"
	"log"
	"os"
	"sync"
)

const (
	SignalForeach  = 1
	SignalReadFile = 2
)

type nodeSignal struct {
	code     uint8
	metadata map[string]interface{}
}

type NodeReturnValue struct {
	Err    error
	Value  map[string]interface{}
	NodeID int
}

type Node struct {
	signalBus chan *nodeSignal
	returnBus chan *NodeReturnValue
	isBusy    bool
	mut       *sync.RWMutex
	id        int
}

func NewNode(id int) *Node {
	w := &Node{
		signalBus: make(chan *nodeSignal),
		isBusy:    false,
		mut:       &sync.RWMutex{},
		id:        id,
	}
	go w.handleSignals()
	return w
}

func (n *Node) SendSignal(s *nodeSignal, returnBus chan *NodeReturnValue) {
	n.mut.Lock()
	defer n.mut.Unlock()
	fmt.Printf("received signal(%d) at node(%d)\n", s.code, n.id)

	n.returnBus = returnBus

	n.isBusy = true
	n.signalBus <- s
}

//	func (n *Node) Recv() *NodeReturnValue {
//		n.mut.Lock()
//		defer n.mut.Unlock()
//
//		return <-n.returnBus
//	}

func (n *Node) pushReturn(ret *NodeReturnValue) {
	ret.NodeID = n.id
	n.returnBus <- ret
}

func (n *Node) handleSignals() {
	for signal := range n.signalBus {
		n.mut.Lock()

		switch signal.code {
		case SignalForeach:
			partition := signal.metadata["partition"].(*Partition)
			fn := signal.metadata["fn"].(func(r *Row))
			n.foreach(partition, fn)
			n.isBusy = false

		case SignalReadFile:
			filePath := signal.metadata["filePath"].(string)
			log.Printf("node(%d) reading file %s\n", n.id, filePath)
			data, err := os.ReadFile(filePath)
			if err != nil {
				log.Printf("unable to read %s : %v\n", filePath, err)
				n.pushReturn(&NodeReturnValue{Err: err})
				n.isBusy = false
				continue
			}

			n.pushReturn(&NodeReturnValue{Value: map[string]interface{}{
				"data":     data,
				"filePath": filePath,
			}})
			n.isBusy = false

		default:
			log.Panicf("received unrecognized code: %d", signal.code)
		}

		n.mut.Unlock()
	}
}

func (n *Node) IsBusy() bool {
	n.mut.RLock()
	defer n.mut.RUnlock()

	return n.isBusy
}

func (n *Node) foreach(partition *Partition, fn func(r *Row)) {
	fmt.Printf("node(%d) executing foreach()\n", n.id)
	for _, row := range partition.Rows {
		fn(row)
	}
}
