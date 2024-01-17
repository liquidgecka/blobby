package logging

import (
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestEncodeJSONString(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	T.Equal(encodeJSONString("abc"), "abc")
	T.Equal(encodeJSONString("a-b=c"), `a-b=c`)
	T.Equal(encodeJSONString(`\`), `\\`)
	T.Equal(encodeJSONString(`"`), `\"`)
	T.Equal(encodeJSONString("\t"), `\t`)
	T.Equal(encodeJSONString("\r"), `\r`)
	T.Equal(encodeJSONString("\n"), `\n`)
	T.Equal(encodeJSONString("\u2028"), `\u2028`)
	T.Equal(encodeJSONString("\u2029"), `\u2029`)
	T.Equal(encodeJSONString("\u0000"), `\u0000`)
}

func TestShouldEscape(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	T.Equal(shouldEscape("aA90_"), false)
	T.Equal(shouldEscape("abc"), false)
	T.Equal(shouldEscape(""), true)
}
