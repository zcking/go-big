package gobig

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestFileNames(t *testing.T) {
	fileNames, err := FileNames("../../data/foods/")
	assert.NoError(t, err)
	expectedFiles := []string{
		"../../data/foods/drinks.json",
		"../../data/foods/fruits.json",
		"../../data/foods/meats.json",
	}
	assert.ElementsMatch(t, fileNames, expectedFiles)
}

func TestDataFrame_ReadFiles_Directory(t *testing.T) {
	df, err := ReadFiles("../../data/foods/")
	assert.NoError(t, err)

	err = df.Foreach(func(row *Row) error {
		fmt.Printf("%s\n", row)
		return nil
	})
	assert.NoError(t, err)
}

func TestDataFrame_ReadFiles_SingleFile(t *testing.T) {
	df, err := ReadFiles("../../data/foods/drinks.json")
	assert.NoError(t, err)

	df.Show()
}

func TestDataFrame_ReadFiles_IsLazy(t *testing.T) {
	_, err := ReadFiles("../../data/foods/")
	assert.NoError(t, err)

	time.Sleep(time.Second * 2)
}
