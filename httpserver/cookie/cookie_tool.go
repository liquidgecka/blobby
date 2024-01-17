package cookie

import (
	"bytes"
	"compress/gzip"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
)

// Handles the encrypting and decrypting of data in cookies.
type CookieTool struct {
	// Returns a list of cipher.Block object that should be used for
	// processing cookies. Cookies will be decrypted with all the keys
	// in order to find one that is valid and will be encrypted with the
	// first in the list. This allows rotation of keys without disruption.
	AESKeys func() ([]cipher.Block, error)
}

// Common hashing function..
func fnvHash(dest []byte, source []byte) {
	f := fnv.New32a()
	f.Write(source)
	sum := f.Sum32()
	dest[0] = byte(sum & 0xFF)
	dest[1] = byte((sum >> 8) & 0xFF)
	dest[2] = byte((sum >> 16) & 0xFF)
	dest[3] = byte((sum >> 24) & 0xFF)
}

// Decodes the given cookie contents into the given interface value. If the
// cookie is not valid or can not be decoded then an error will be returned.
func (c *CookieTool) Decode(cookie string, v interface{}) error {
	// Convert the string we were given into a base64 decoded value. This
	// removes the base64 encoding and gives us the raw underlying
	// data.
	rawCookie, err := base64.RawStdEncoding.DecodeString(cookie)
	if err != nil {
		return fmt.Errorf("Invalid base64 encoded value.")
	}

	// First we need to find the decryption key that works. Since
	// we have multiple keys (current and old) we need to walk through
	// each attempting to decrypt the key to find the sentinel.
	keys, err := c.AESKeys()
	if err != nil {
		return err
	}
	for _, candidate := range keys {
		// If the raw data is not perfectly aligned with the block size
		// then this cipher is clearly not compatible.
		bs := candidate.BlockSize()
		if len(rawCookie)%bs != 0 {
			continue
		}

		// We can not decrypt in place because doing so will erase the
		// contents which prevents us from decoding a later candidate
		// cipher.
		buffer := make([]byte, len(rawCookie))
		for i := 0; i < len(buffer); i += bs {
			candidate.Decrypt(buffer[i:], rawCookie[i:])
		}

		// If the hash does not match then we need to roll over to trying
		// the next encryption key.
		hash := [4]byte{}
		fnvHash(hash[:], buffer[4:])
		if !bytes.Equal(buffer[0:4], hash[:]) {
			continue
		}

		// Next we need to unzip it, then json unmarshal it.
		unzipper, err := gzip.NewReader(bytes.NewBuffer(buffer[4:]))
		if err != nil {
			continue
		}
		decoder := json.NewDecoder(unzipper)
		if err := decoder.Decode(v); err != nil {
			continue
		}

		// Success!
		return nil
	}

	// No valid cookie was found.
	return fmt.Errorf("Cookie was not valid.")
}

// Like decodeCookie except this is used for encoding.
func (c *CookieTool) Encode(v interface{}) (string, error) {
	out := bytes.Buffer{}
	out.Grow(4096)
	// Write a place holder that we will use for storing our hash once
	// we have gotten the full binary data later.
	out.Write([]byte{0, 0, 0, 0})
	zipper := gzip.NewWriter(&out)
	encoder := json.NewEncoder(zipper)
	if err := encoder.Encode(v); err != nil {
		panic(err)
	} else if err = zipper.Close(); err != nil {
		panic(err)
	}
	keys, err := c.AESKeys()
	if err != nil {
		return "", err
	}
	cipher := keys[0]
	bs := cipher.BlockSize()
	add := bs - (out.Len() % bs)
	if add > 0 && add < bs {
		addBytes := make([]byte, add)
		out.Write(addBytes)
	}
	raw := out.Bytes()
	rawLen := len(raw)
	fnvHash(raw, raw[4:])
	for i := 0; i < rawLen; i += bs {
		segment := raw[i : i+bs]
		cipher.Encrypt(segment, segment)
	}
	return base64.RawStdEncoding.EncodeToString(raw), nil
}
