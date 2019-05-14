package memfs

type Comparable interface {
	Compare(to Comparable) int
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
