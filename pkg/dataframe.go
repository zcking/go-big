package pkg

import (
	"time"
)

var (
	NumNodes = GetEnvInt("NUM_NODES", 5)
)

type DataFrame struct {
	Partitions []*Partition
	Nodes      []*Node
}

func NewDataFrame(data []map[string]interface{}) (*DataFrame, error) {
	// Naively split the data into a few different chunks, aka partitions
	// NOTE: this is just for demonstration purposes, it's obviously not realistic
	var partitions []*Partition

	if len(data) >= 2 {
		rightIdx := len(data) / 2

		leftHalf := data[:rightIdx]
		rightHalf := data[rightIdx:]
		partitions = append(partitions, NewPartition(leftHalf), NewPartition(rightHalf))
	} else {
		partitions = append(partitions, NewPartition(data))
	}

	// Now create a pool of nodes to represent our distributed nodes
	// of parallel execution, like a cluster.
	nodes := make([]*Node, NumNodes)
	for i := 0; i < NumNodes; i++ {
		nodes[i] = NewNode(i)
	}

	return &DataFrame{
		Partitions: partitions,
		Nodes:      nodes,
	}, nil
}

func (df *DataFrame) NextAvailableNode() *Node {
	for {
		for _, node := range df.Nodes {
			if !node.IsBusy() {
				return node
			}
		}
	}
}

func (df *DataFrame) Wait() {
	for {
		allFinished := true
		for _, node := range df.Nodes {
			allFinished = allFinished && !node.IsBusy()

			if node.IsBusy() {
				time.Sleep(time.Nanosecond * 100)
			}
		}

		if allFinished {
			break
		}
	}
}
