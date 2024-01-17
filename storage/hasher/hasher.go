package hasher

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/minio/highwayhash"
)

var (
	// This key was randomly generated but needs to be consistent between
	// builds.
	highwayHashKey = [32]byte{
		0x07, 0xd7, 0x50, 0xde, 0x39, 0xc4, 0x4c, 0xae,
		0x47, 0x3b, 0x98, 0x8e, 0x5e, 0xb6, 0x7a, 0x31,
		0xa4, 0xab, 0x6c, 0x0b, 0xda, 0xac, 0x47, 0x71,
		0x2a, 0xfc, 0x01, 0x37, 0x3a, 0x09, 0x81, 0xc8,
	}
)

type Hasher struct {
	dest     io.Writer
	hash     hash.Hash
	validate string
	expect   []byte
	typ      string
}

// Initializes a hasher that will generate a hash header based on the
// data provided.
func Computer(typ string, out io.Writer) (*Hasher, error) {
	switch typ {
	case "hh":
		hh, _ := highwayhash.New64(highwayHashKey[:])
		return &Hasher{
			dest: out,
			hash: hh,
			typ:  typ,
		}, nil
	case "md5":
		return &Hasher{
			dest: out,
			hash: md5.New(),
			typ:  typ,
		}, nil
	default:
		return nil, fmt.Errorf("Unknown hash type: %s", typ)
	}
}

// Initializes the hasher to validate a given hash string. This will check
// the properties of the string and setup the hash accordingly. The intention
// is that this can eventually support multiple types of hashes based
// on the string being passed in.
func Validator(s string, out io.Writer) (*Hasher, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("Unable to determine hash type.")
	}
	expect, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("Unable to decode hash value: %s", err.Error())
	}
	switch parts[0] {
	case "hh":
		hh, _ := highwayhash.New64(highwayHashKey[:])
		return &Hasher{
			dest:     out,
			expect:   expect,
			hash:     hh,
			typ:      parts[0],
			validate: s,
		}, nil
	case "md5":
		return &Hasher{
			dest:     out,
			expect:   expect,
			hash:     md5.New(),
			typ:      parts[0],
			validate: s,
		}, nil
	default:
		return nil, fmt.Errorf("Unknown hash type: %s", parts[0])
	}
}

// Returns true if the string passed to Initialize() matches the sum of the
// hash of the data written to the hasher.
func (h *Hasher) Check() bool {
	sum := h.hash.Sum(nil)
	if len(sum) != len(h.expect) {
		return false
	}
	for i, v := range sum {
		if v != h.expect[i] {
			return false
		}
	}
	return true
}

// Returns the generated hash as a string that can be passed to Initialize.
func (h *Hasher) Hash() string {
	return h.typ + "=" + base64.RawURLEncoding.EncodeToString(h.hash.Sum(nil))
}

func (h *Hasher) Write(data []byte) (n int, err error) {
	n, err = h.dest.Write(data)
	if err == nil {
		h.hash.Write(data[:n])
	}
	return
}
