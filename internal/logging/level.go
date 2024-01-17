package logging

type level int

const (
	dbg = level(iota)
	inf
	wrn
	err
)
