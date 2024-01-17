package hasher

import (
	"bytes"
	"crypto/md5"
	"io/ioutil"
	"testing"

	"github.com/liquidgecka/testlib"
	"github.com/minio/highwayhash"
)

func TestComputer(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	out := bytes.Buffer{}
	hh, err := highwayhash.New64(highwayHashKey[:])
	T.ExpectSuccess(err)
	T.NotEqual(hh, nil)

	// Highway Hash
	h, err := Computer("hh", &out)
	T.ExpectSuccess(err)
	T.Equal(h, &Hasher{
		dest: &out,
		hash: hh,
		typ:  "hh",
	})

	// MD5 Hash
	h, err = Computer("md5", &out)
	T.ExpectSuccess(err)
	T.Equal(h, &Hasher{
		dest: &out,
		hash: md5.New(),
		typ:  "md5",
	})

	// Invalid
	h, err = Computer("invalid", &out)
	T.ExpectErrorMessage(err, "Unknown hash type: invalid")
	T.Equal(h, nil)
}

func TestValidator(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	out := bytes.Buffer{}
	hh, err := highwayhash.New64(highwayHashKey[:])
	T.ExpectSuccess(err)
	T.NotEqual(hh, nil)

	// no =, expect error.
	h, err := Validator("test", &out)
	T.ExpectErrorMessage(err, "Unable to determine hash type.")
	T.Equal(h, nil)

	// Invalid hash base64 value.
	h, err = Validator("a=|", &out)
	T.ExpectErrorMessage(
		err,
		"Unable to decode hash value: illegal base64 data at input byte 0")
	T.Equal(h, nil)

	// Invalid hash type.
	h, err = Validator("test=BASE64", &out)
	T.ExpectErrorMessage(err, "Unknown hash type: test")
	T.Equal(h, nil)

	// Highway Hash
	h, err = Validator("hh=AAAAAAAAAAA", &out)
	T.ExpectSuccess(err)
	T.Equal(h, &Hasher{
		dest:     &out,
		expect:   []byte{0, 0, 0, 0, 0, 0, 0, 0},
		hash:     hh,
		typ:      "hh",
		validate: "hh=AAAAAAAAAAA",
	})

	// MD5
	h, err = Validator("md5=AAAAAAAAAAAAAAAAAAAAAA", &out)
	T.ExpectSuccess(err)
	T.Equal(h, &Hasher{
		dest:     &out,
		expect:   []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		hash:     md5.New(),
		typ:      "md5",
		validate: "md5=AAAAAAAAAAAAAAAAAAAAAA",
	})
}

func TestHasher_Check_MD5(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	str := "test"
	dst := bytes.NewBuffer(make([]byte, 0, len(str)))
	h := Hasher{
		typ: "md5",
		expect: []byte{
			0x09, 0x8f, 0x6b, 0xcd, 0x46, 0x21, 0xd3, 0x73,
			0xca, 0xde, 0x4e, 0x83, 0x26, 0x27, 0xb4, 0xf6,
		},
		hash: md5.New(),
		dest: dst,
	}
	n, err := h.Write([]byte(str))
	T.Equal(n, len(str))
	T.ExpectSuccess(err)
	T.Equal(dst.Bytes(), []byte(str))
	T.Equal(h.Check(), true)
	h.expect = []byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	}
	T.Equal(h.Check(), false)
	h.expect = []byte{0x00}
	T.Equal(h.Check(), false)
}

func TestHasher_Check_HighwayHash(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	str := "test"
	dst := bytes.NewBuffer(make([]byte, 0, len(str)))
	hh, _ := highwayhash.New64(highwayHashKey[:])
	h := Hasher{
		typ: "hh",
		expect: []byte{
			0x7c, 0x76, 0xC7, 0x2F, 0x1D, 0xDC, 0x48, 0xB1,
		},
		hash: hh,
		dest: dst,
	}
	n, err := h.Write([]byte(str))
	T.Equal(n, len(str))
	T.ExpectSuccess(err)
	T.Equal(dst.Bytes(), []byte(str))
	T.Equal(h.Check(), true)
	h.expect = []byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	}
	T.Equal(h.Check(), false)
	h.expect = []byte{0xFF}
	T.Equal(h.Check(), false)
}

func TestHasher_Hash_MD5(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Check against a simple string.
	str := "test"
	dst := bytes.NewBuffer(make([]byte, 0, len(str)))
	h := Hasher{
		typ:  "md5",
		hash: md5.New(),
		dest: dst,
	}
	n, err := h.Write([]byte(str))
	T.Equal(n, len(str))
	T.ExpectSuccess(err)
	T.Equal(dst.Bytes(), []byte(str))
	T.Equal(h.Hash(), "md5=CY9rzUYh03PK3k6DJie09g")

	// Check with a large array.
	h = Hasher{
		typ:  "md5",
		hash: md5.New(),
		dest: ioutil.Discard,
	}
	buffer := make([]byte, 1024*1024)
	for i := range buffer {
		buffer[i] = byte(i % 256)
	}
	for i := 1; i < len(buffer); i <<= 1 {
		n, err := h.Write(buffer[0:i])
		T.Equal(n, i)
		T.ExpectSuccess(err)
	}
	T.Equal(h.Hash(), "md5=JGlsfQ_SvKhi11G8Ko_zgA")
}

func TestHasher_Hash_HighwayHash(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Check against a simple string.
	str := "test"
	dst := bytes.NewBuffer(make([]byte, 0, len(str)))
	hh, _ := highwayhash.New64(highwayHashKey[:])
	h := Hasher{
		typ:  "hh",
		hash: hh,
		dest: dst,
	}
	n, err := h.Write([]byte(str))
	T.Equal(n, len(str))
	T.ExpectSuccess(err)
	T.Equal(dst.Bytes(), []byte(str))
	T.Equal(h.Hash(), "hh=fHbHLx3cSLE")

	// Check with a large array.
	hh, _ = highwayhash.New64(highwayHashKey[:])
	h = Hasher{
		typ:  "hh",
		hash: hh,
		dest: ioutil.Discard,
	}
	buffer := make([]byte, 1024*1024)
	for i := range buffer {
		buffer[i] = byte(i % 256)
	}
	for i := 1; i < len(buffer); i <<= 1 {
		n, err := h.Write(buffer[0:i])
		T.Equal(n, i)
		T.ExpectSuccess(err)
	}
	T.Equal(h.Hash(), "hh=qxRxHq0KnCM")
}
