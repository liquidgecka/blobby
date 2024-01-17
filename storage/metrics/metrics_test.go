package metrics

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/liquidgecka/testlib"
)

func fuzzValue(T *testlib.T, v reflect.Value) {
	switch v.Kind() {
	case reflect.Float64:
		v.Set(reflect.ValueOf(rand.Float64()))
	case reflect.Int:
		v.Set(reflect.ValueOf(rand.Int()))
	case reflect.Int8:
		v.Set(reflect.ValueOf(int8(rand.Int31())))
	case reflect.Int16:
		v.Set(reflect.ValueOf(int16(rand.Int31())))
	case reflect.Int32:
		v.Set(reflect.ValueOf(rand.Int31()))
	case reflect.Int64:
		v.Set(reflect.ValueOf(rand.Int63()))
	case reflect.Uint:
		v.Set(reflect.ValueOf(uint(rand.Uint32())))
	case reflect.Uint8:
		v.Set(reflect.ValueOf(uint8(rand.Uint32())))
	case reflect.Uint16:
		v.Set(reflect.ValueOf(uint16(rand.Uint32())))
	case reflect.Uint32:
		v.Set(reflect.ValueOf(rand.Uint32()))
	case reflect.Uint64:
		v.Set(reflect.ValueOf(rand.Uint64()))

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			fuzzValue(T, v.Field(i))
		}
	default:
		T.Fatalf("The Metrics{} object has a field with an unknown type.")
	}
}

func TestMetrics_CopyFrom(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Create a metrics object and populate it with random values.
	// This will help ensure that the copy makes a true copy and no
	// field is getting missed. Note that we make a copy of the want
	// value before copying just in case the copy alters the source rather
	// than the destination.
	want := Metrics{}
	fuzzValue(T, reflect.Indirect(reflect.ValueOf(&want)))
	source := want
	have := Metrics{}
	have.CopyFrom(&source)
	T.Equal(&have, &want)
}

func TestMetricFailedSuccessTotal_IncFailures(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	m := MetricFailedSuccessTotal{}
	m.IncFailures()
	T.Equal(m.Failures, int64(1))
	m.IncFailures()
	T.Equal(m.Failures, int64(2))
}

func TestMetricFailedSuccessTotal_IncSuccesses(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	m := MetricFailedSuccessTotal{}
	m.IncSuccesses()
	T.Equal(m.Successes, int64(1))
	m.IncSuccesses()
	T.Equal(m.Successes, int64(2))
}

func TestMetricFailedSuccessTotal_IncTotal(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	m := MetricFailedSuccessTotal{}
	m.IncTotal()
	T.Equal(m.Total, int64(1))
	m.IncTotal()
	T.Equal(m.Total, int64(2))
}
