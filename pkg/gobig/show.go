package gobig

import (
	"fmt"
	"log"
	"strings"
)

func (df *DataFrame) Show() {
	columnsString := strings.Join(df.Columns, " | ")
	columnsHeader := fmt.Sprintf("[| %s |]\n", columnsString)
	fmt.Print(columnsHeader)
	fmt.Printf(strings.Repeat("-", len(columnsHeader)))
	fmt.Println()

	err := df.PushStep(&ShowExecution{})
	if err != nil {
		log.Fatal(err)
	}
}

type ShowExecution struct {
}

func (e *ShowExecution) Execute(n *Node) *NodeReturnValue {

	for _, part := range n.Partitions {
		if part == nil {
			continue
		}

		for _, row := range part.Rows {
			fmt.Printf("%s\n", row)
		}
	}
	return nil
}

func (e *ShowExecution) IsLazy() bool {
	return false
}

func (e *ShowExecution) String() string {
	return "Show"
}
