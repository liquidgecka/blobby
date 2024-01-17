package human

import (
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestBytes(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// B
	T.Equal(Bytes(0), "0B")
	T.Equal(Bytes(1), "1B")
	T.Equal(Bytes(10), "10B")
	T.Equal(Bytes(100), "100B")
	T.Equal(Bytes(999), "999B")

	// kB
	T.Equal(Bytes(1000), "1kB")
	T.Equal(Bytes(1001), "1kB")
	T.Equal(Bytes(1009), "1kB")
	T.Equal(Bytes(1010), "1.01kB")
	T.Equal(Bytes(1019), "1.01kB")
	T.Equal(Bytes(1100), "1.1kB")
	T.Equal(Bytes(1110), "1.11kB")
	T.Equal(Bytes(1111), "1.11kB")
	T.Equal(Bytes(10000), "10kB")
	T.Equal(Bytes(10001), "10kB")
	T.Equal(Bytes(10099), "10kB")
	T.Equal(Bytes(10100), "10.1kB")
	T.Equal(Bytes(99999), "99.9kB")
	T.Equal(Bytes(100000), "100kB")
	T.Equal(Bytes(999999), "999kB")

	// MB
	T.Equal(Bytes(1000000), "1MB")
	T.Equal(Bytes(1010000), "1.01MB")
	T.Equal(Bytes(1100000), "1.1MB")
	T.Equal(Bytes(9999999), "9.99MB")
	T.Equal(Bytes(10000000), "10MB")
	T.Equal(Bytes(10100000), "10.1MB")
	T.Equal(Bytes(99999999), "99.9MB")
	T.Equal(Bytes(100000000), "100MB")
	T.Equal(Bytes(100100000), "100MB")
	T.Equal(Bytes(999999999), "999MB")

	// GB
	T.Equal(Bytes(1000000000), "1GB")
	T.Equal(Bytes(1010000000), "1.01GB")
	T.Equal(Bytes(1100000000), "1.1GB")
	T.Equal(Bytes(9999999999), "9.99GB")
	T.Equal(Bytes(10000000000), "10GB")
	T.Equal(Bytes(10100000000), "10.1GB")
	T.Equal(Bytes(99999999999), "99.9GB")
	T.Equal(Bytes(100000000000), "100GB")
	T.Equal(Bytes(100100000000), "100GB")
	T.Equal(Bytes(999999999999), "999GB")

	// TB
	T.Equal(Bytes(1000000000000), "1TB")
	T.Equal(Bytes(1010000000000), "1.01TB")
	T.Equal(Bytes(1100000000000), "1.1TB")
	T.Equal(Bytes(9999999999999), "9.99TB")
	T.Equal(Bytes(10000000000000), "10TB")
	T.Equal(Bytes(10100000000000), "10.1TB")
	T.Equal(Bytes(99999999999999), "99.9TB")
	T.Equal(Bytes(100000000000000), "100TB")
	T.Equal(Bytes(100100000000000), "100TB")
	T.Equal(Bytes(999999999999999), "999TB")

	// PB
	T.Equal(Bytes(1000000000000000), "1PB")
	T.Equal(Bytes(1010000000000000), "1.01PB")
	T.Equal(Bytes(1100000000000000), "1.1PB")
	T.Equal(Bytes(9999999999999999), "9.99PB")
	T.Equal(Bytes(10000000000000000), "10PB")
	T.Equal(Bytes(10100000000000000), "10.1PB")
	T.Equal(Bytes(99999999999999999), "99.9PB")
	T.Equal(Bytes(100000000000000000), "100PB")
	T.Equal(Bytes(100100000000000000), "100PB")
	T.Equal(Bytes(999999999999999999), "999PB")

	// EB
	T.Equal(Bytes(1000000000000000000), "1EB")
	T.Equal(Bytes(1010000000000000000), "1.01EB")
	T.Equal(Bytes(1100000000000000000), "1.1EB")
	T.Equal(Bytes(9999999999999999999), "9.99EB")
	T.Equal(Bytes(10000000000000000000), "10EB")
	T.Equal(Bytes(10100000000000000000), "10.1EB")
	T.Equal(Bytes(18446744073709551615), "18.4EB")
}
