package fid

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/liquidgecka/testlib"
)

func TestFID_String(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// These are just some static examples that were precomputed in order
	// to catch any major oops situations.

	// Case 1
	f := FID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	T.Equal(f.String(), "AAAAAAAAAAAAAA")

	// Case 2
	f = FID{255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
	T.Equal(f.String(), "_____________w")

	// Case 2
	f = FID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	T.Equal(f.String(), "AQIDBAUGBwgJCg")
}

func TestFID_Generate(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Some precomputed examples. Note that we have to adjust the
	// global fidID value so that it doesn't change randomly and we need
	// to mock out the time.Now call using monkey patching.
	mockTime := time.Date(2020, time.February, 20, 2, 2, 2, 2, time.UTC)
	patch := monkey.Patch(time.Now, func() time.Time {
		return mockTime
	})
	defer patch.Unpatch()
	f := FID{}

	// Test 1
	fidID = 0
	f.Generate(2)
	// 4 bytes = time (above), 2 = fidID, 4 = machine id (provided)
	T.Equal(f, FID{94, 77, 232, 154, 0, 1, 0, 0, 0, 2})
	T.Equal(f.String(), "Xk3omgABAAAAAg")

	// Test 2
	fidID = 0xFFFE
	f.Generate(0xFFFFFFFF)
	// 4 bytes = time (above), 2 = fidID, 4 = machine id (provided)
	T.Equal(f, FID{94, 77, 232, 154, 255, 255, 255, 255, 255, 255})
	T.Equal(f.String(), "Xk3omv_______w")
}

func TestFID_ID(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Base fid to work with.
	f := FID{0, 1, 2, 3, 4, 5, 6, 7, 8, 10}

	// Simple 0 / 0 short example.
	T.Equal(f.ID(0, 0), "AAECAwQFBgcICgAAAAAAAAAA")
	T.Equal(f.ID(0xFFFFFFFF, 0), "AAECAwQFBgcICv____8AAAAA")
	T.Equal(f.ID(0xFFFFFFFF, 0xFFFFFFFF), "AAECAwQFBgcICv__________")
	T.Equal(
		f.ID(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFF),
		"AAECAwQFBgcICv_______________w")
}

func TestFID_Machine(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Try a default/simple case.
	f := FID{}
	f.Generate(0)
	T.Equal(f.Machine(), uint32(0))

	// Try 100 random cases.
	for i := 0; i < 100; i++ {
		id := rand.Uint32()
		f.Generate(id)
		T.Equal(f.Machine(), id, fmt.Sprintf("id=%d", id))
	}
}

func TestFID_Parse(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Generate a fid, then ensure it can be Parsed back.
	f := FID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	newf := FID{}
	T.ExpectSuccess(newf.Parse(f.String()))
	T.Equal(newf, f)

	// Ensure that a long input fails.
	T.ExpectErrorMessage(
		newf.Parse("AAAAAAAAAAAAAAAAAA"),
		"Invalid FID string")

	// Ensure that a short input fails as well.
	T.ExpectErrorMessage(
		newf.Parse("AAA"),
		"Invalid FID string")

	// And that an invalid base64 error is returned as well.
	T.ExpectErrorMessage(
		newf.Parse("&____________A"),
		"illegal base64 data at input byte 0")
}

func TestParseID(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Parse invalid base64 and ensure that it fails.
	hfid, hStart, hLength, err := ParseID("\000")
	T.ExpectErrorMessage(err, "illegal base64 data")
	T.Equal(hfid, FID{})
	T.Equal(hStart, uint64(0))
	T.Equal(hLength, uint32(0))

	// And test that valid base64 data that is the wrong length is also
	// disallowed.
	hfid, hStart, hLength, err = ParseID("aaa")
	T.ExpectErrorMessage(err, "Not a valid ID token.")
	T.Equal(hfid, FID{})
	T.Equal(hStart, uint64(0))
	T.Equal(hLength, uint32(0))

	// Generate a fid and an ID from that source.
	wfid := FID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	id := wfid.ID(0, 0)

	// Parse the id given to ensure that it returns expected values.
	hfid, hStart, hLength, err = ParseID(id)
	T.ExpectSuccess(err)
	T.Equal(hfid, wfid)
	T.Equal(hStart, uint64(0))
	T.Equal(hLength, uint32(0))

	// Now try the same thing for 100 "short" ids, and 100 "long" ids.
	for i := 0; i < 100; i++ {
		wStart := rand.Uint64()
		if i >= 100 {
			wStart = wStart & 0xFFFFFFFF
		}
		wLength := rand.Uint32()
		tName := fmt.Sprintf("start=%d length=%d", wStart, wLength)
		id := hfid.ID(wStart, wLength)
		hfid, hStart, hLength, err = ParseID(id)
		T.ExpectSuccess(err, tName)
		T.Equal(hfid, wfid, tName)
		T.Equal(hStart, wStart, tName)
		T.Equal(hLength, wLength, tName)
	}
}
