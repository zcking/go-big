package gobig

func (df *DataFrame) Foreach(fn func(*Row) error) error {
	return df.PushStep(&ForeachExecution{
		Fn: fn,
	})
}

type ForeachExecution struct {
	Fn func(r *Row) error
}

func (e *ForeachExecution) Execute(n *Node) *NodeReturnValue {
	//fmt.Printf("node(%d) starting execution on %d partitions\n", n.ID, len(n.Partitions))
	for _, part := range n.Partitions {
		if part == nil {
			continue
		}

		//fmt.Printf("node(%d) executing foreach()\n", n.ID)
		for _, row := range part.Rows {
			if err := e.Fn(row); err != nil {
				return &NodeReturnValue{
					Err: err,
				}
			}
		}
	}
	return nil
}

func (e *ForeachExecution) IsLazy() bool {
	return false
}

func (e *ForeachExecution) String() string {
	return "Foreach"
}
