package hasher

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"strings"
	"sync"

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

	hhPool = sync.Pool{
		New: func() interface{} {
			hh, _ := highwayhash.New64(highwayHashKey[:])
			return hh
		},
	}
)

type Hasher struct {
	dest     io.Writer
	hash     hash.Hash
	validate string
	expect   []byte
	typ      string
}

func getHighwayHash() hash.Hash {
	hh := hhPool.Get().(hash.Hash)
	hh.Reset()
	return hh
}

// Free returns internal pooled resources (such as highwayhash state)
// back to the pool for reuse.
func (h *Hasher) Free() {
	if h != nil && h.typ == "hh" && h.hash != nil {
		hhPool.Put(h.hash)
		h.hash = nil
	}
}

// Initializes a hasher that will generate a hash header based on the
// data provided.
func Computer(typ string, out io.Writer) (*Hasher, error) {
	switch typ {
	case "hh":
		return &Hasher{
			dest: out,
			hash: getHighwayHash(),
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
	var typ, b64val string
	if strings.HasPrefix(s, "hh=") {
		typ = "hh"
		b64val = s[3:]
	} else if strings.HasPrefix(s, "md5=") {
		typ = "md5"
		b64val = s[4:]
	} else {
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			return nil, fmt.Errorf("Unable to determine hash type.")
		}
		typ = s[:eq]
		b64val = s[eq+1:]
	}

	expectLen := base64.RawURLEncoding.DecodedLen(len(b64val))
	expect := make([]byte, expectLen)
	n, err := base64.RawURLEncoding.Decode(expect, []byte(b64val))
	if err != nil {
		return nil, fmt.Errorf("Unable to decode hash value: %s", err.Error())
	}
	expect = expect[:n]

	switch typ {
	case "hh":
		return &Hasher{
			dest:     out,
			expect:   expect,
			hash:     getHighwayHash(),
			typ:      typ,
			validate: s,
		}, nil
	case "md5":
		return &Hasher{
			dest:     out,
			expect:   expect,
			hash:     md5.New(),
			typ:      typ,
			validate: s,
		}, nil
	default:
		return nil, fmt.Errorf("Unknown hash type: %s", typ)
	}
}

// Returns true if the string passed to Initialize() matches the sum of the
// hash of the data written to the hasher.
func (h *Hasher) Check() bool {
	var buf [16]byte
	sum := h.hash.Sum(buf[:0])
	return bytes.Equal(sum, h.expect)
}

// Returns the generated hash as a string that can be passed to Initialize.
func (h *Hasher) Hash() string {
	if h.typ == "hh" {
		var sum [8]byte
		s := h.hash.Sum(sum[:0])
		var out [14]byte
		copy(out[:3], "hh=")
		base64.RawURLEncoding.Encode(out[3:], s)
		return string(out[:])
	}
	var sum [16]byte
	s := h.hash.Sum(sum[:0])
	encLen := base64.RawURLEncoding.EncodedLen(len(s))
	buf := make([]byte, len(h.typ)+1+encLen)
	copy(buf, h.typ)
	buf[len(h.typ)] = '='
	base64.RawURLEncoding.Encode(buf[len(h.typ)+1:], s)
	return string(buf)
}

func (h *Hasher) Write(data []byte) (n int, err error) {
	n, err = h.dest.Write(data)
	if err == nil {
		h.hash.Write(data[:n])
	}
	return
}
