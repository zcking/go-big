package pkg

import (
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

func (df *DataFrame) ReadFiles(filePathOrPrefix string) error {
	// Start by determining if the provided argument is a single file or a directory.
	// If it is a directory, list the files located there, and we'll have each Node
	// handle reading one file at a time
	files := []string{filePathOrPrefix}
	if IsDirectory(filePathOrPrefix) {
		f, err := FileNames(filePathOrPrefix)
		if err != nil {
			return err
		}
		files = f
	}

	// For demonstration purposes we can print out the read-in data
	// asynchronously as it comes in from the nodes
	nodeResponses := make(chan *NodeReturnValue, len(files))
	go func() {
		fmt.Println("awaiting responses...")
		for resp := range nodeResponses {
			if resp.Err != nil {
				log.Fatal(resp.Err)
			}
			fileData := resp.Value["data"].([]byte)
			filePath := resp.Value["filePath"].(string)
			fmt.Printf("node(%v) read file(%s):\n%s\n\n", resp.NodeID, filePath, string(fileData))
		}
		fmt.Println("done")
	}()

	for _, file := range files {
		// Find the next available node that can process our request
		node := df.NextAvailableNode()

		// Prepare the signal to inform the node of our execution
		signal := &nodeSignal{
			code: SignalReadFile,
			metadata: map[string]interface{}{
				"filePath": file,
			},
		}

		node.SendSignal(signal, nodeResponses)
	}

	// Wait for all nodes to be finished
	df.Wait()
	return nil
}
