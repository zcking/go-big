package pkg

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestFileNames(t *testing.T) {
	fileNames, err := FileNames("../data/foods/")
	assert.NoError(t, err)
	assert.ElementsMatch(t, fileNames, []string{"../data/foods/fruits.json", "../data/foods/meats.json"})
}

func TestDataFrame_ReadFiles_Directory(t *testing.T) {
	df := NewEmptyDataFrame()
	err := df.ReadFiles("../data/foods/")
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second * 2)
}
