package gobig

func chunkSlice[V interface{}](slice []V, chunkSize int, guaranteeCapacity int) [][]V {
	chunks := make([][]V, guaranteeCapacity)
	chunkIdx := 0

	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}

		chunks[chunkIdx] = slice[i:end]
		chunkIdx++
	}

	return chunks
}
