package pkg

import (
	"fmt"
	"log"
	"sync"
)

const (
	SignalForeach = 1
)

type nodeSignal struct {
	code     uint8
	metadata map[string]interface{}
}

type Node struct {
	signalBus chan *nodeSignal
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

func (w *Node) SendSignal(s *nodeSignal) {
	w.mut.Lock()
	defer w.mut.Unlock()

	w.isBusy = true
	w.signalBus <- s
}

func (w *Node) handleSignals() {
	for signal := range w.signalBus {
		w.mut.Lock()

		switch signal.code {
		case SignalForeach:
			partition := signal.metadata["partition"].(*Partition)
			fn := signal.metadata["fn"].(func(r *Row))
			w.foreach(partition, fn)
			w.isBusy = false
		default:
			log.Panicf("received unrecognized code: %d", signal.code)
		}

		w.mut.Unlock()
	}
}

func (w *Node) IsBusy() bool {
	w.mut.RLock()
	defer w.mut.RUnlock()

	return w.isBusy
}

func (w *Node) foreach(partition *Partition, fn func(r *Row)) {
	fmt.Printf("node(%d) executing foreach()\n", w.id)
	for _, row := range partition.Rows {
		fn(row)
	}
}
