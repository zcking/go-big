package gobig

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataFrame_Foreach(t *testing.T) {
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
		{
			"name": "charlotte",
			"age":  36,
		},
	}

	df, err := NewDataFrame(rawData)
	assert.NoError(t, err)

	err = df.Foreach(func(r *Row) error {
		name := r.GetString("name")
		age := r.GetInt("age")
		fmt.Printf("Name = %s, Age = %d\n", name, age)
		return nil
	})
	assert.NoError(t, err)
}
