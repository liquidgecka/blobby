package histogram

type int64Slice []int64

func (i int64Slice) Len() int {
	return len(i)
}

func (i int64Slice) Less(x, y int) bool {
	return i[x] < i[y]
}

func (i int64Slice) Swap(x, y int) {
	i[x], i[y] = i[y], i[x]
}
