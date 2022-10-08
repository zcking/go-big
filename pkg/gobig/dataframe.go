package gobig

import (
	"fmt"
	"log"
	"time"
)

var (
	NumNodes = GetEnvInt("NUM_NODES", 5)
)

type DataFrame struct {
	Columns           []string
	NumPartitions     int
	Nodes             []*Node
	plan              []ExecutionStep
	lastCompletedStep int
}

func NewEmptyDataFrame() *DataFrame {
	df, err := NewDataFrame([]map[string]interface{}{})
	if err != nil {
		log.Panicf("%v", err)
	}
	return df
}

func NewDataFrame(data []map[string]interface{}) (*DataFrame, error) {
	// Naively split the data into a few different chunks, aka Partitions
	// NOTE: this is just for demonstration purposes, it's obviously not realistic.
	// For now we're just going to divide the data up evenly among the nodes
	rowCountPerNode := len(data) / NumNodes
	if rowCountPerNode <= 0 {
		rowCountPerNode = 1
	}
	dataChunks := chunkSlice(data, rowCountPerNode, NumNodes)
	fmt.Printf("NumNodes=%d, numChunks=%d\n", NumNodes, len(dataChunks))
	partitions := make([]*Partition, NumNodes)
	for i, chunk := range dataChunks {
		partitions[i] = NewPartition(chunk)
	}

	// Now create a pool of nodes to represent our distributed nodes
	// of parallel execution, like a cluster.
	nodes := make([]*Node, NumNodes)
	for i := 0; i < NumNodes; i++ {
		partitionsForThisNode := []*Partition{
			partitions[i],
		}
		nodes[i] = NewNode(i, partitionsForThisNode)
	}

	var columns []string
	if len(data) > 0 {
		firstRow := data[0]
		columns = make([]string, len(firstRow))
		columnIdx := 0
		for k := range firstRow {
			columns[columnIdx] = k
			columnIdx++
		}
	}

	return &DataFrame{
		Columns:           columns,
		Nodes:             nodes,
		lastCompletedStep: -1,
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

func (df *DataFrame) PushStep(step ExecutionStep) error {
	df.plan = append(df.plan, step)

	if !step.IsLazy() {
		// This step is one that triggers evaluation
		return df.Execute()
	}

	return nil
}

func (df *DataFrame) Execute() error {
	if len(df.plan) == 0 {
		return fmt.Errorf("there is nothing to execute")
	}

	for i, step := range df.plan {
		// For demonstration purposes we can print out the read-in data
		// asynchronously as it comes in from the nodes
		nodeResponses := make(chan *NodeReturnValue, len(df.Nodes))
		go func() {
			//fmt.Printf("awaiting executions of step(%s)...\n", step.String())
			for resp := range nodeResponses {
				if resp.Err != nil {
					log.Fatal(resp.Err)
				}
			}
		}()

		for _, node := range df.Nodes {
			node.SendExecution(step, nodeResponses)
		}
		df.Wait()
		df.lastCompletedStep = i
	}
	return nil
}
