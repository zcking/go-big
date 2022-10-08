package gobig

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func IsDirectory(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.IsDir()
}

func FileNames(directory string) ([]string, error) {
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	filePaths := make([]string, len(files))
	for i, file := range files {
		filePaths[i] = filepath.Join(directory, file.Name())
	}
	return filePaths, nil
}

func ReadFiles(filePathOrPrefix string) (*DataFrame, error) {
	df := NewEmptyDataFrame()

	// Start by determining if the provided argument is a single file or a directory.
	// If it is a directory, list the files located there, and we'll have each Node
	// handle reading one file at a time
	files := []string{filePathOrPrefix}
	if IsDirectory(filePathOrPrefix) {
		f, err := FileNames(filePathOrPrefix)
		if err != nil {
			return nil, err
		}
		files = f
	}

	// Our ReadFiles feature will distribute itself by dividing the number of
	// files needed to be read, by the number of nodes we have. This is still
	// a naive algorithm since some files may be larger than others, but we'll keep it simple for now.
	if len(files) == 0 {
		return nil, fmt.Errorf("file or directory not found: %s", filePathOrPrefix)
	}

	filesPerNode := len(df.Nodes) / len(files)
	if filesPerNode <= 0 {
		filesPerNode = 1
	}
	groupedFiles := chunkSlice(files, filesPerNode, len(df.Nodes))

	if err := df.PushStep(&ReadFilesExecution{
		AllFilePaths: groupedFiles,
	}); err != nil {
		return nil, err
	}

	df.Columns = []string{"data"}
	return df, nil
}

type ReadFilesExecution struct {
	AllFilePaths [][]string
}

func (e *ReadFilesExecution) Execute(n *Node) *NodeReturnValue {
	filePaths := e.AllFilePaths[n.ID]
	//fmt.Printf("node(%d) filePaths = %v\n", n.ID, filePaths)

	partitions := make([]*Partition, len(filePaths))
	for fileIdx, filePath := range filePaths {
		//log.Printf("node(%d) reading file %s\n", n.ID, filePath)

		// For now our file reader is very basic; we do not do any parsing
		// or figuring out what the format of the file is.
		// So the way we'll store the data in the node's partitions is simply as a string (naive)
		// where each line of text in the file is considered a Row.
		rowsFromTextFile, err := e.readLines(filePath)
		if err != nil {
			log.Printf("unable to read %s : %v\n", filePath, err)
			return &NodeReturnValue{Err: err}
		}

		partitions[fileIdx] = NewPartitionFromRows(rowsFromTextFile)
	}

	// Now we initialize the partitions on the given Node, setting them equal to the
	// data we read in from the file
	n.Partitions = partitions

	return nil
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func (e *ReadFilesExecution) readLines(path string) ([]*Row, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var rows []*Row
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		rowForTextLine := NewRow(map[string]interface{}{
			"data": scanner.Text(),
		})
		rows = append(rows, rowForTextLine)
	}
	return rows, scanner.Err()
}

func (e *ReadFilesExecution) IsLazy() bool {
	return true
}

func (e *ReadFilesExecution) String() string {
	return "ReadFiles"
}
