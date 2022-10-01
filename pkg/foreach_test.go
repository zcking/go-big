package pkg

import (
	"fmt"
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

	df, _ := NewDataFrame(rawData)
	df.Foreach(func(r *Row) {
		name := r.GetString("name")
		age := r.GetInt("age")
		fmt.Printf("Name = %s, Age = %d\n", name, age)
	})
}
