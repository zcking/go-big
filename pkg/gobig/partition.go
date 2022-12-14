package gobig

type Partition struct {
	Rows []*Row
}

func NewPartition(data []map[string]interface{}) *Partition {
	rows := make([]*Row, len(data))
	for i, r := range data {
		rows[i] = NewRow(r)
	}

	return &Partition{
		Rows: rows,
	}
}

func NewPartitionFromRows(rows []*Row) *Partition {
	return &Partition{
		Rows: rows,
	}
}
