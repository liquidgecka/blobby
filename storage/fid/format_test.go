package fid

import (
	"testing"

	"github.com/liquidgecka/testlib"
)

var (
	// 1970/1/1 00:00:00
	epoch     = FID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	hour1     = FID{0, 0, 14, 16, 0, 0, 0, 0}
	hour12    = FID{0, 0, 168, 192, 0, 0, 0, 0}
	day1      = FID{0, 1, 81, 128, 0, 0, 0, 0}
	day2      = FID{0, 2, 163, 0, 0, 0, 0, 0}
	day3      = FID{0, 3, 244, 128, 0, 0, 0, 0}
	day4      = FID{0, 5, 70, 0, 0, 0, 0, 0}
	day5      = FID{0, 6, 151, 128, 0, 0, 0, 0}
	day6      = FID{0, 7, 233, 0, 0, 0, 0, 0}
	day10     = FID{0, 11, 221, 128, 0, 0, 0, 0}
	month2    = FID{0, 40, 222, 128, 0, 0, 0, 0}
	month3    = FID{0, 77, 200, 128, 0, 0, 0, 0}
	month4    = FID{0, 118, 167, 0, 0, 0, 0, 0}
	month5    = FID{0, 158, 52, 0, 0, 0, 0, 0}
	month6    = FID{0, 199, 18, 128, 0, 0, 0, 0}
	month7    = FID{0, 238, 159, 128, 0, 0, 0, 0}
	month8    = FID{1, 23, 126, 0, 0, 0, 0, 0}
	month9    = FID{1, 64, 92, 128, 0, 0, 0, 0}
	month10   = FID{1, 103, 233, 128, 0, 0, 0, 0}
	month11   = FID{1, 144, 200, 0, 0, 0, 0, 0}
	month12   = FID{1, 184, 85, 0, 0, 0, 0, 0}
	year2000  = FID{56, 109, 67, 128, 0, 0, 0, 0}
	localhost = FID{0, 0, 0, 0, 255, 255, 127, 0, 0, 1}
)

func TestFormatter_Static(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`static_string`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "static_string")
}

func TestFormatter_StaticEscapes(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`static%%%n%tstring`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "static%\n\tstring")
}

func TestFormatter_AMPM(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`%p %P`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "am AM")
	T.Equal(f.Format(hour12), "pm PM")
}

func TestFormatter_Century(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`%C`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "19")
	T.Equal(f.Format(year2000), "20")
}

func TestFormatter_Day(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`'%d' '%-d' '%_d'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "'01' '1' ' 1'")
}

func TestFormatter_Default(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f := &Formatter{}
	T.Equal(f.Format(epoch), "AAAAAAAAAAAAAA")
	f = nil
	T.Equal(f.Format(epoch), "AAAAAAAAAAAAAA")
}

func TestFormatter_Epoch(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`%s`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "0")
}

func TestFormater_Errors(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	_, err := NewFormatter("%*")
	T.ExpectErrorMessage(err, "%*")
	_, err = NewFormatter("%_*")
	T.ExpectErrorMessage(err, "%_*")
	_, err = NewFormatter("%-*")
	T.ExpectErrorMessage(err, "%-*")
	_, err = NewFormatter("%.*")
	T.ExpectErrorMessage(err, "%.*")
	_, err = NewFormatter("%")
	T.ExpectErrorMessage(err, "Unterminated escape")
}

func TestFormatter_Hour(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`'%H' '%-H' '%_H'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "'00' '0' ' 0'")
	T.Equal(f.Format(hour1), "'01' '1' ' 1'")
	f, err = NewFormatter(`'%I' '%-I' '%_I'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "'12' '12' '12'")
	T.Equal(f.Format(hour1), "'01' '1' ' 1'")
}

func TestFormatter_ID(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`'%K' '%-K' '%_K'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "'00000' '0' '    0'")
	T.Equal(f.Format(localhost), "'65535' '65535' '65535'")
}

func TestFormatter_Minute(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`'%M' '%-M' '%_M'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "'00' '0' ' 0'")
}

func TestFormatter_Month(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`%b %B '%m' '%-m' '%_m'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "Jan January '01' '1' ' 1'")
	T.Equal(f.Format(month2), "Feb February '02' '2' ' 2'")
	T.Equal(f.Format(month3), "Mar March '03' '3' ' 3'")
	T.Equal(f.Format(month4), "Apr April '04' '4' ' 4'")
	T.Equal(f.Format(month5), "May May '05' '5' ' 5'")
	T.Equal(f.Format(month6), "Jun June '06' '6' ' 6'")
	T.Equal(f.Format(month7), "Jul July '07' '7' ' 7'")
	T.Equal(f.Format(month8), "Aug August '08' '8' ' 8'")
	T.Equal(f.Format(month9), "Sep September '09' '9' ' 9'")
	T.Equal(f.Format(month10), "Oct October '10' '10' '10'")
	T.Equal(f.Format(month11), "Nov November '11' '11' '11'")
	T.Equal(f.Format(month12), "Dec December '12' '12' '12'")
}

func TestFormatter_Machine(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`'%L' '%-L' '%_L' '%.L'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "'0000000000' '0' '         0' '0.0.0.0'")
	T.Equal(
		f.Format(localhost),
		"'2130706433' '2130706433' '2130706433' '127.0.0.1'")
}

func TestFormatter_Quarter(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`%q`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "1")
}

func TestFormater_Specials(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// %F
	f, err := NewFormatter(`%F`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "1970-01-01")
	T.Equal(f.Format(day1), "1970-01-02")

	// %r
	f, err = NewFormatter(`%r`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "12:00:00 AM")
	T.Equal(f.Format(hour12), "12:00:00 PM")

	// %R
	f, err = NewFormatter(`%R`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "00:00")
	T.Equal(f.Format(hour12), "12:00")

	// %T
	f, err = NewFormatter(`%T`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "00:00:00")
	T.Equal(f.Format(hour12), "12:00:00")
}

func TestFormatter_Second(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`'%S' '%-S' '%_S'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "'00' '0' ' 0'")
}

func TestFormatter_Weekday(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`%a %A %u %w`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "Thu Thursday 5 4")
	T.Equal(f.Format(day1), "Fri Friday 6 5")
	T.Equal(f.Format(day2), "Sat Saturday 7 6")
	T.Equal(f.Format(day3), "Sun Sunday 1 0")
	T.Equal(f.Format(day4), "Mon Monday 2 1")
	T.Equal(f.Format(day5), "Tue Tuesday 3 2")
	T.Equal(f.Format(day6), "Wed Wednesday 4 3")
}

func TestFormatter_Year(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`%y '%Y' '%_Y' '%-Y'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "1970 '70' '70' '70'")
	T.Equal(f.Format(year2000), "2000 '00' ' 0' '0'")
}

func TestFormatter_YearDay(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	f, err := NewFormatter(`%j '%j' '%_j' '%-j'`)
	T.ExpectSuccess(err)
	T.Equal(f.Format(epoch), "001 '001' '  1' '1'")
	T.Equal(f.Format(day10), "010 '010' ' 10' '10'")
}
