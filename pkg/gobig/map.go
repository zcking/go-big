package gobig

import "log"

func (df *DataFrame) Map(fn func(*Row) *Row) *DataFrame {
	err := df.PushStep(&MapExecution{
		Fn: fn,
	})
	if err != nil {
		log.Fatal(err)
	}
	return df
}

type MapExecution struct {
	Fn func(r *Row) *Row
}

func (e *MapExecution) Execute(n *Node) *NodeReturnValue {
	newPartitions := make([]*Partition, len(n.Partitions))

	for partIdx, part := range n.Partitions {
		if part == nil {
			continue
		}

		transformed := make([]*Row, len(part.Rows))

		for i, row := range part.Rows {
			transformed[i] = e.Fn(row)
		}
		newPartitions[partIdx] = NewPartitionFromRows(transformed)
	}

	n.Partitions = newPartitions
	return nil
}

func (e *MapExecution) IsLazy() bool {
	return true
}

func (e *MapExecution) String() string {
	return "Map"
}
