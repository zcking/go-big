package gobig

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataFrame_Show(t *testing.T) {
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

	err = df.Show()
	assert.NoError(t, err)
}
