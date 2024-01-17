package cookie

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"testing"

	"bou.ke/monkey"
	"github.com/liquidgecka/testlib"
)

func TestCookieTool(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	want := struct {
		Test1 string `json:"t1"`
		Test2 string `json:"t2"`
	}{}
	have := want
	want.Test1 = "test1"
	want.Test2 = "test2"

	aesRaw := [32]byte{}
	a, err := aes.NewCipher(aesRaw[:])
	var ae error
	T.ExpectSuccess(err)
	aesKeys := []cipher.Block{a}
	ct := CookieTool{
		AESKeys: func() ([]cipher.Block, error) {
			return aesKeys, ae
		},
	}

	// Encode the cookie.
	raw, err := ct.Encode(&want)
	T.ExpectSuccess(err)
	// Value computed elsewhere and encoded static here.
	T.Equal(raw, ""+
		`FiOo7nkEyy9fj1fSRCn2adBp0pC3TZXzFZ1j2cJTpDs`+
		`Xbuxx1VggtgOT35wNGefG3JXAeKJAiYmtSKIUkoQghw`)

	// Decode using a single AES key.
	T.ExpectSuccess(ct.Decode(raw, &have))
	T.Equal(have, want)

	// Add a AES key at the start of the list, then try again which will
	// force the data to get decoded twice.
	nk := [32]byte{}
	for i := range nk {
		nk[i] = 0xFF
	}
	a2, err := aes.NewCipher(nk[:])
	T.ExpectSuccess(err)
	aesKeys = []cipher.Block{a2, a}
	T.ExpectSuccess(ct.Decode(raw, &have))
	T.Equal(have, want)
}

type testCipher struct {
	blockSize func() int
	decrypt   func([]byte, []byte)
	encrypt   func([]byte, []byte)
}

func (t *testCipher) BlockSize() int {
	return t.blockSize()
}

func (t *testCipher) Decrypt(in, out []byte) {
	t.decrypt(in, out)
}

func (t *testCipher) Encrypt(in, out []byte) {
	t.encrypt(in, out)
}

func TestCookieTool_Decode(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Test 1: Invalid base64 encoded data.
	ct := CookieTool{}
	T.ExpectErrorMessage(
		ct.Decode("****", struct{}{}),
		"Invalid base64 encoded value.",
	)

	// Test 2: AESKeys function returns an error.
	ct = CookieTool{AESKeys: func() ([]cipher.Block, error) {
		return nil, fmt.Errorf("EXPECTED")
	}}
	T.ExpectErrorMessage(ct.Decode("AAAAAA", struct{}{}), "EXPECTED")

	// Test 3: The cipher can not be used because the raw data size is not
	// a multiple of the block size.
	bsCalled := false
	aeskeys := func() ([]cipher.Block, error) {
		return []cipher.Block{
			&testCipher{
				blockSize: func() int {
					bsCalled = true
					return 1000
				},
				decrypt: func(in, out []byte) {
					T.Fatal("This should not be called.")
				},
			},
		}, nil
	}
	ct = CookieTool{AESKeys: aeskeys}
	T.ExpectErrorMessage(
		ct.Decode("AAAAAAAAAAAA", struct{}{}),
		"Cookie was not valid.")
	T.Equal(bsCalled, true)

	// Test 4: The data hash is not valid.
	bsCalled = false
	decryptCalls := 0
	aeskeys = func() ([]cipher.Block, error) {
		return []cipher.Block{
			&testCipher{
				blockSize: func() int {
					bsCalled = true
					return 1
				},
				decrypt: func(out, in []byte) {
					copy(out, in)
					decryptCalls += 1
				},
			},
		}, nil
	}
	func() {
		defer monkey.Patch(
			gzip.NewReader,
			func(io.Reader) (*gzip.Reader, error) {
				T.Fatal("gzip shouldn't be called.")
				panic("NOT REACHED")
			},
		).Unpatch()

		raw := base64.RawStdEncoding.EncodeToString([]byte("bad_hash_value"))
		ct = CookieTool{AESKeys: aeskeys}
		T.ExpectErrorMessage(
			ct.Decode(raw, struct{}{}),
			"Cookie was not valid.")
		T.Equal(bsCalled, true)
		T.Equal(decryptCalls, len("bad_hash_value"))
	}()

	// Test 5: The data decrypts but is not gzipped.
	bsCalled = false
	decryptCalls = 0
	source := []byte("\x00\x00\x00\x00not_valid_gzip")
	h := fnv.New32a()
	h.Write(source[4:])
	hsum := h.Sum(nil)
	source[0] = hsum[3]
	source[1] = hsum[2]
	source[2] = hsum[1]
	source[3] = hsum[0]
	raw := base64.RawStdEncoding.EncodeToString(source)
	ct = CookieTool{AESKeys: aeskeys}
	T.ExpectErrorMessage(
		ct.Decode(raw, struct{}{}),
		"Cookie was not valid.")
	T.Equal(bsCalled, true)
	T.Equal(decryptCalls, len(source))

	// Test 6: Valid gzip data but not valid json.
	bsCalled = false
	decryptCalls = 0
	out := bytes.NewBuffer([]byte{0, 0, 0, 0})
	gzipper := gzip.NewWriter(out)
	_, err := gzipper.Write([]byte("not_valid_json"))
	T.ExpectSuccess(err)
	T.ExpectSuccess(gzipper.Close())
	source = out.Bytes()
	h = fnv.New32a()
	h.Write(source[4:])
	hsum = h.Sum(nil)
	source[0] = hsum[3]
	source[1] = hsum[2]
	source[2] = hsum[1]
	source[3] = hsum[0]
	raw = base64.RawStdEncoding.EncodeToString(source)
	ct = CookieTool{AESKeys: aeskeys}
	T.ExpectErrorMessage(
		ct.Decode(raw, struct{}{}),
		"Cookie was not valid.")
	T.Equal(bsCalled, true)
	T.Equal(decryptCalls, len(source))
}
