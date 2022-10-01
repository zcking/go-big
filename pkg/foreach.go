package pkg

import "fmt"

func (df *DataFrame) Foreach(fn func(r *Row)) {
	// For each partition in the dataframe
	// Execute "fn" against each row in that partition.
	// Each partition is handled by a separate node, hence
	// a distributed foreach transformation
	for _, part := range df.Partitions {
		// Find the next available node that can process our request
		node := df.NextAvailableNode()

		// Prepare the signal to inform the node of our execution
		signal := &nodeSignal{
			code: SignalForeach,
			metadata: map[string]interface{}{
				"fn":        fn,
				"partition": part,
			},
		}

		fmt.Printf("sending signal to node(%d)\n", node.id)
		node.SendSignal(signal)
	}

	// Wait for all nodes to be finished
	df.Wait()
}
