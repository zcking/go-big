package pkg

type Row map[string]interface{}

func NewRow(fields map[string]interface{}) *Row {
	r := &Row{}
	for k, v := range fields {
		(*r)[k] = v
	}
	return r
}

func (r *Row) GetInt(column string) int {
	return (*r)[column].(int)
}

func (r *Row) GetString(column string) string {
	return (*r)[column].(string)
}

func (r *Row) GetBool(column string) bool {
	return (*r)[column].(bool)
}

func (r *Row) GetFloat(column string) float64 {
	return (*r)[column].(float64)
}
