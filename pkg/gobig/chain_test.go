package gobig

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestDataFrame_ChainedMethods(t *testing.T) {
	df, err := ReadFiles("../../data/foods/fruits.json")
	assert.NoError(t, err)

	type Food struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	df.
		Map(func(r *Row) *Row {
			txt := r.GetString("data")
			food := &Food{}
			if err = json.Unmarshal([]byte(txt), food); err != nil {
				t.Error(err)
			}

			return NewRow(map[string]interface{}{
				"name":          strings.ToTitle(food.Name),
				"readTimestamp": time.Now().Format(time.RFC822),
			})
		}).
		Map(func(r *Row) *Row {
			name := r.GetString("name")
			(*r)["name"] = name[:len(name)-2] + "..."
			return r
		}).
		Show()
}
