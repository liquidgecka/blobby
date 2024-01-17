package cookie

import (
	"testing"
	"time"

	"github.com/liquidgecka/testlib"
)

func TestTime(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	marshalTests := map[string]time.Time{
		"0":            time.Unix(0, 0),
		"0.000000001":  time.Unix(0, 1),
		"0.999999999":  time.Unix(0, 999999999),
		"1.000000001":  time.Unix(1, 1),
		"1604121273.1": time.Unix(1604121273, 100000000),
	}
	for str, stamp := range marshalTests {
		tm := Time{stamp}
		raw, err := tm.MarshalText()
		T.ExpectSuccess(err, "test: "+str)
		T.Equal(string(raw), str, "test: "+str)
		tm2 := Time{}
		T.ExpectSuccess(tm2.UnmarshalText(raw), "test: "+str)
		T.Equal(tm, tm2, "test: "+str)
	}
}
