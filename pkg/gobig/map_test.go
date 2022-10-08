package gobig

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestDataFrame_Map(t *testing.T) {
	rawData := []map[string]interface{}{
		{
			"name": "bruce",
			"age":  26,
		},
		{
			"name": "denise",
			"age":  49,
		},
		{
			"name": "gregory",
			"age":  28,
		},
	}

	df, err := NewDataFrame(rawData)
	assert.NoError(t, err)

	// Transform the dataframe by upper-casing the 'name' field
	df.Map(func(row *Row) *Row {
		name := row.GetString("name")
		age := row.GetInt("age")

		(*row)["name"] = strings.ToUpper(name)
		(*row)["age"] = age * 2
		return row
	}).Show()
	assert.NoError(t, err)
}
