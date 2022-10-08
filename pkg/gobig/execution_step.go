package gobig

type ExecutionStep interface {
	// Execute is the implementation function that will be serialized and sent to
	// the other nodes for execution on their data
	Execute(n *Node) *NodeReturnValue
	// IsLazy flags whether this step is lazily evaluated or triggers execution
	IsLazy() bool
	// String implements the Stringer for the execution step
	String() string
}
