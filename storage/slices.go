package storage

import (
	"strings"
)

type primarySlice []*primary

func (p primarySlice) Len() int {
	return len(p)
}

func (p primarySlice) Less(i, j int) bool {
	return strings.Compare(p[i].fidStr, p[j].fidStr) < 0
}

func (p primarySlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type replicaSlice []*replica

func (p replicaSlice) Len() int {
	return len(p)
}

func (p replicaSlice) Less(i, j int) bool {
	return strings.Compare(p[i].fidStr, p[j].fidStr) < 0
}

func (p replicaSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
