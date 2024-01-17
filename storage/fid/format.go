package fid

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Formatter struct {
	// The following values are used to establish if a given format requires
	// various data to be precomputed. This is useful to ensure that various
	// values are not extracted if not needed.
	requiresTime    bool
	requiresMachine bool
	requiresID      bool
	requiresString  bool

	// The maximal size of the string that will be generated. This is just
	// a guess so we can preallocate space in the strings.Builder.
	maxSize int

	// The format functions are each passed data about the fid and expected
	// to append string data into the given string buffer.
	funcs []func(*fmtData, *strings.Builder)
}

// Takes a FID and returns the formatted string using the configured
// Formatter.
func (f *Formatter) Format(fid FID) string {
	if f == nil || f.funcs == nil {
		return fid.String()
	}
	data := fmtData{}
	if f.requiresTime {
		epoch := (0 +
			int64(fid[0])<<24 +
			int64(fid[1])<<16 +
			int64(fid[2])<<8 +
			int64(fid[3]))
		data.created = time.Unix(epoch, 0).In(time.UTC)
	}
	if f.requiresMachine {
		data.machine = (0 +
			uint32(fid[6])<<24 +
			uint32(fid[7])<<16 +
			uint32(fid[8])<<8 +
			uint32(fid[9]))
	}
	if f.requiresID {
		data.id = (uint16(fid[4])<<8 + uint16(fid[5]))
	}
	b := strings.Builder{}
	b.Grow(f.maxSize)
	for _, fun := range f.funcs {
		fun(&data, &b)
	}
	return b.String()
}

// Creates a new formatter from the given format string. If the string is
// not a valid format syntax then this will return an error.
func NewFormatter(s string) (*Formatter, error) {
	f := &Formatter{}
	p := parser{
		f: f,
	}

	// Walk the characters processing them via the state machine.
	for i, r := range s {
		if p.next == nil {
			if r == '%' {
				p.next = p.escaped
			} else {
				p.static.WriteRune(r)
			}
		} else if err := p.next(i, r); err != nil {
			return nil, err
		}
	}

	// If p.next is not nil then we ended in an escape sequence which
	// is not valid.
	if p.next != nil {
		return nil, fmt.Errorf("Unterminated escape sequence.")
	}

	// Add any remaining static data if needed.
	p.addStatic()

	// Copy the max allocation size and return the results.
	f.maxSize = p.maxSize
	return f, nil
}

// A parser that will convert a passed in string into a Formater. This
// is a very simple state machine used for simple processing so we can
// walk the string generating.
type parser struct {
	f       *Formatter
	static  strings.Builder
	next    func(int, rune) error
	maxSize int
}

// Adds all of the buffered static data to the function list as a static
// string generator.
func (p *parser) addStatic() {
	if p.static.Len() == 0 {
		return
	}
	p.maxSize += p.static.Len()
	p.f.funcs = append(
		p.f.funcs,
		staticString(p.static.String()).Format)
	p.static.Reset()
}

// The state that handles all escape sequences at the top level (immediately
// after a % sign.)
func (p *parser) escaped(i int, r rune) error {
	switch r {
	case '%':
		p.static.WriteRune('%')
		p.next = nil
		return nil
	case 'n':
		p.static.WriteRune('\n')
		p.next = nil
		return nil
	case 't':
		p.static.WriteRune('\t')
		p.next = nil
		return nil
	}
	p.addStatic()
	switch r {
	case '.':
		p.next = p.period
		return nil
	case '_':
		p.next = p.underscore
		return nil
	case '-':
		p.next = p.hyphen
		return nil
	case 'a':
		p.maxSize += 3
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatWeekdayAbbreviatedName)
	case 'A':
		p.maxSize += 9
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatWeekdayName)
	case 'b':
		p.maxSize += 3
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatMonthAbbreviatedName)
	case 'B':
		p.maxSize += 9
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatMonthName)
	case 'C':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatCentury)
	case 'd':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatDayZero)
	case 'F':
		p.maxSize += 10
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatYear)
		p.f.funcs = append(p.f.funcs, staticString("-").Format)
		p.f.funcs = append(p.f.funcs, formatMonthZero)
		p.f.funcs = append(p.f.funcs, staticString("-").Format)
		p.f.funcs = append(p.f.funcs, formatDayZero)
	case 'H':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHourZero)
	case 'I':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHourAMPMZero)
	case 'j':
		p.maxSize += 3
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatYearDayZero)
	case 'K':
		p.maxSize += 5
		p.f.requiresID = true
		p.f.funcs = append(p.f.funcs, formatIDZero)
	case 'L':
		p.maxSize += 10
		p.f.requiresMachine = true
		p.f.funcs = append(p.f.funcs, formatMachineZero)
	case 'm':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatMonthZero)
	case 'M':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatMinuteZero)
	case 'p':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatAMPMLower)
	case 'P':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatAMPM)
	case 'q':
		p.maxSize += 1
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatQuarter)
	case 'r':
		p.maxSize += 13
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHourAMPMZero)
		p.f.funcs = append(p.f.funcs, staticString(":").Format)
		p.f.funcs = append(p.f.funcs, formatMinuteZero)
		p.f.funcs = append(p.f.funcs, staticString(":").Format)
		p.f.funcs = append(p.f.funcs, formatSecondZero)
		p.f.funcs = append(p.f.funcs, staticString(" ").Format)
		p.f.funcs = append(p.f.funcs, formatAMPM)
	case 'R':
		p.maxSize += 5
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHourZero)
		p.f.funcs = append(p.f.funcs, staticString(":").Format)
		p.f.funcs = append(p.f.funcs, formatMinuteZero)
	case 's':
		p.maxSize += 12
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatEpoch)
	case 'S':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatSecondZero)
	case 'T':
		p.maxSize += 8
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHourZero)
		p.f.funcs = append(p.f.funcs, staticString(":").Format)
		p.f.funcs = append(p.f.funcs, formatMinuteZero)
		p.f.funcs = append(p.f.funcs, staticString(":").Format)
		p.f.funcs = append(p.f.funcs, formatSecondZero)
	case 'u':
		p.maxSize += 1
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatWeekdayNumber)
	case 'w':
		p.maxSize += 1
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatWeekdayNumberZeroStart)
	case 'y':
		p.maxSize += 4
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatYear)
	case 'Y':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatYearOfCenturyZero)
	default:
		return fmt.Errorf(
			"Unknown or unsupported escape sequence %%%c at %d",
			r,
			i)
	}
	p.next = nil
	return nil
}

// The state called for the character following a %-.
func (p *parser) hyphen(i int, r rune) error {
	switch r {
	case 'd':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatDay)
	case 'H':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHour)
	case 'I':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHourAMPM)
	case 'j':
		p.maxSize += 3
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatYearDay)
	case 'K':
		p.maxSize += 5
		p.f.requiresID = true
		p.f.funcs = append(p.f.funcs, formatID)
	case 'L':
		p.maxSize += 10
		p.f.requiresMachine = true
		p.f.funcs = append(p.f.funcs, formatMachine)
	case 'm':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatMonth)
	case 'M':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatMinute)
	case 'S':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatSecond)
	case 'Y':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatYearOfCentury)
	default:
		return fmt.Errorf(
			"Unknown or unsupported escape sequence %%-%c at %d",
			r,
			i)
	}
	p.next = nil
	return nil
}

func (p *parser) period(i int, r rune) error {
	switch r {
	case 'L':
		p.maxSize += 15
		p.f.requiresMachine = true
		p.f.funcs = append(p.f.funcs, formatMachineIP)
	default:
		return fmt.Errorf(
			"Unknown or unsupported escape sequence %%.%c at %d",
			r,
			i)
	}
	p.next = nil
	return nil
}

// Processes an escape sequence that has followed a '%_' sequence.
func (p *parser) underscore(i int, r rune) error {
	switch r {
	case 'd':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatDaySpace)
	case 'H':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHourSpace)
	case 'I':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatHourAMPMSpace)
	case 'j':
		p.maxSize += 3
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatYearDaySpace)
	case 'K':
		p.maxSize += 5
		p.f.requiresID = true
		p.f.funcs = append(p.f.funcs, formatIDSpace)
	case 'L':
		p.maxSize += 5
		p.f.requiresMachine = true
		p.f.funcs = append(p.f.funcs, formatMachineSpace)
	case 'm':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatMonthSpace)
	case 'M':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatMinuteSpace)
	case 'S':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatSecondSpace)
	case 'Y':
		p.maxSize += 2
		p.f.requiresTime = true
		p.f.funcs = append(p.f.funcs, formatYearOfCenturySpace)
	default:
		return fmt.Errorf(
			"Unknown or unsupported escape sequence %%_%c at %d",
			r,
			i)
	}
	p.next = nil
	return nil
}

// Used for writing a static string into a format.
type staticString string

func (s staticString) Format(d *fmtData, out *strings.Builder) {
	out.WriteString(string(s))
}

// Stores a copy of the data that can be used for generating string data
// about the fid.
type fmtData struct {
	created time.Time
	machine uint32
	id      uint16
}

// Appends either AM or PM depending on the time.
func formatAMPM(d *fmtData, out *strings.Builder) {
	if d.created.Hour() < 12 {
		out.WriteString("AM")
	} else {
		out.WriteString("PM")
	}
}

// Appends either am or pm depending on the time.
func formatAMPMLower(d *fmtData, out *strings.Builder) {
	if d.created.Hour() < 12 {
		out.WriteString("am")
	} else {
		out.WriteString("pm")
	}
}

// Appends the Century.
func formatCentury(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(d.created.Year() / 100))
}

// Appends the day of the month.
func formatDay(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(d.created.Day()))
}

// Appends the day of the month zero padded.
func formatDaySpace(d *fmtData, out *strings.Builder) {
	day := d.created.Day()
	if day < 10 {
		out.WriteRune(' ')
	}
	out.WriteString(strconv.Itoa(day))
}

// Appends the day of the month zero padded.
func formatDayZero(d *fmtData, out *strings.Builder) {
	day := d.created.Day()
	if day < 10 {
		out.WriteRune('0')
	}
	out.WriteString(strconv.Itoa(day))
}

// Appends the number of seconds elapsed since 1970/1/1
func formatEpoch(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.FormatInt(d.created.Unix(), 10))
}

// Appends the hour of the day.
func formatHour(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(d.created.Hour()))
}

// Appends the hour of the day zero padded.
func formatHourSpace(d *fmtData, out *strings.Builder) {
	hour := d.created.Hour()
	if hour < 10 {
		out.WriteRune(' ')
	}
	out.WriteString(strconv.Itoa(hour))
}

// Appends the hour of the day zero padded.
func formatHourZero(d *fmtData, out *strings.Builder) {
	hour := d.created.Hour()
	if hour < 10 {
		out.WriteRune('0')
	}
	out.WriteString(strconv.Itoa(hour))
}

// Appends the hour of the day in am/pm format.
func formatHourAMPM(d *fmtData, out *strings.Builder) {
	hour := d.created.Hour() % 12
	if hour == 0 {
		hour = 12
	}
	out.WriteString(strconv.Itoa(hour))
}

// Appends the hour of the day in am/pm format zero padded.
func formatHourAMPMSpace(d *fmtData, out *strings.Builder) {
	hour := d.created.Hour() % 12
	if hour == 0 {
		hour = 12
	}
	if hour < 10 {
		out.WriteRune(' ')
	}
	out.WriteString(strconv.Itoa(hour))
}

// Appends the hour of the day in am/pm format zero padded.
func formatHourAMPMZero(d *fmtData, out *strings.Builder) {
	hour := d.created.Hour() % 12
	if hour == 0 {
		hour = 12
	}
	if hour < 10 {
		out.WriteRune('0')
	}
	out.WriteString(strconv.Itoa(hour))
}

// Appends the ID as a raw number.
func formatID(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(int(d.id)))
}

// Appends the ID as a space padded number.
func formatIDSpace(d *fmtData, out *strings.Builder) {
	id := strconv.Itoa(int(d.id))
	for i := len(id); i < 5; i++ {
		out.WriteRune(' ')
	}
	out.WriteString(id)
}

// Appends the ID as a zero padded number.
func formatIDZero(d *fmtData, out *strings.Builder) {
	id := strconv.Itoa(int(d.id))
	for i := len(id); i < 5; i++ {
		out.WriteRune('0')
	}
	out.WriteString(id)
}

// Appends the machine as a raw number.
func formatMachine(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.FormatUint(uint64(d.machine), 10))
}

// Appends the machine as a space padded number.
func formatMachineIP(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(int((d.machine >> 24) & 0xFF)))
	out.WriteRune('.')
	out.WriteString(strconv.Itoa(int((d.machine >> 16) & 0xFF)))
	out.WriteRune('.')
	out.WriteString(strconv.Itoa(int((d.machine >> 8) & 0xFF)))
	out.WriteRune('.')
	out.WriteString(strconv.Itoa(int(d.machine & 0xFF)))
}

// Appends the machine as a space padded number.
func formatMachineSpace(d *fmtData, out *strings.Builder) {
	id := strconv.FormatUint(uint64(d.machine), 10)
	for i := len(id); i < 10; i++ {
		out.WriteRune(' ')
	}
	out.WriteString(id)
}

// Appends the machine as a zero padded number.
func formatMachineZero(d *fmtData, out *strings.Builder) {
	id := strconv.FormatUint(uint64(d.machine), 10)
	for i := len(id); i < 10; i++ {
		out.WriteRune('0')
	}
	out.WriteString(id)
}

// Appends the minute of the hour.
func formatMinute(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(d.created.Minute()))
}

// Appends the minute of the hour zero padded.
func formatMinuteSpace(d *fmtData, out *strings.Builder) {
	minute := d.created.Minute()
	if minute < 10 {
		out.WriteRune(' ')
	}
	out.WriteString(strconv.Itoa(minute))
}

// Appends the minute of the hour zero padded.
func formatMinuteZero(d *fmtData, out *strings.Builder) {
	minute := d.created.Minute()
	if minute < 10 {
		out.WriteRune('0')
	}
	out.WriteString(strconv.Itoa(minute))
}

// Appends the month as a number.
func formatMonth(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(int(d.created.Month())))
}

// Appends the month as space padded number.
func formatMonthSpace(d *fmtData, out *strings.Builder) {
	month := int(d.created.Month())
	if month < 10 {
		out.WriteRune(' ')
	}
	out.WriteString(strconv.Itoa(month))
}

// Appends the month as a zero padded number.
func formatMonthZero(d *fmtData, out *strings.Builder) {
	month := int(d.created.Month())
	if month < 10 {
		out.WriteRune('0')
	}
	out.WriteString(strconv.Itoa(month))
}

// Appends the month name (January, February, etc.)
func formatMonthName(d *fmtData, out *strings.Builder) {
	switch d.created.Month() {
	case time.January:
		out.WriteString("January")
	case time.February:
		out.WriteString("February")
	case time.March:
		out.WriteString("March")
	case time.April:
		out.WriteString("April")
	case time.May:
		out.WriteString("May")
	case time.June:
		out.WriteString("June")
	case time.July:
		out.WriteString("July")
	case time.August:
		out.WriteString("August")
	case time.September:
		out.WriteString("September")
	case time.October:
		out.WriteString("October")
	case time.November:
		out.WriteString("November")
	case time.December:
		out.WriteString("December")
	}
}

// Appends the abbreviated month name (Jan, Feb, etc.)
func formatMonthAbbreviatedName(d *fmtData, out *strings.Builder) {
	switch d.created.Month() {
	case time.January:
		out.WriteString("Jan")
	case time.February:
		out.WriteString("Feb")
	case time.March:
		out.WriteString("Mar")
	case time.April:
		out.WriteString("Apr")
	case time.May:
		out.WriteString("May")
	case time.June:
		out.WriteString("Jun")
	case time.July:
		out.WriteString("Jul")
	case time.August:
		out.WriteString("Aug")
	case time.September:
		out.WriteString("Sep")
	case time.October:
		out.WriteString("Oct")
	case time.November:
		out.WriteString("Nov")
	case time.December:
		out.WriteString("Dec")
	}
}

// Appends the quarter as a number.
func formatQuarter(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa((int(d.created.Month()) / 3) + 1))
}

// Appends the seconds in the minute.
func formatSecond(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(d.created.Second()))
}

// Appends the second of the minute zero padded.
func formatSecondSpace(d *fmtData, out *strings.Builder) {
	nanoseconds := d.created.Second()
	if nanoseconds < 10 {
		out.WriteRune(' ')
	}
	out.WriteString(strconv.Itoa(nanoseconds))
}

// Appends the second of the minute zero padded.
func formatSecondZero(d *fmtData, out *strings.Builder) {
	nanoseconds := d.created.Second()
	if nanoseconds < 10 {
		out.WriteRune('0')
	}
	out.WriteString(strconv.Itoa(nanoseconds))
}

// Appends the weekday name (Sunday, Monday, ...)
func formatWeekdayName(d *fmtData, out *strings.Builder) {
	switch d.created.Weekday() {
	case time.Sunday:
		out.WriteString("Sunday")
	case time.Monday:
		out.WriteString("Monday")
	case time.Tuesday:
		out.WriteString("Tuesday")
	case time.Wednesday:
		out.WriteString("Wednesday")
	case time.Thursday:
		out.WriteString("Thursday")
	case time.Friday:
		out.WriteString("Friday")
	case time.Saturday:
		out.WriteString("Saturday")
	}
}

// Appends the abbreviated weekday name (Sun, Mon, ...)
func formatWeekdayAbbreviatedName(d *fmtData, out *strings.Builder) {
	switch d.created.Weekday() {
	case time.Sunday:
		out.WriteString("Sun")
	case time.Monday:
		out.WriteString("Mon")
	case time.Tuesday:
		out.WriteString("Tue")
	case time.Wednesday:
		out.WriteString("Wed")
	case time.Thursday:
		out.WriteString("Thu")
	case time.Friday:
		out.WriteString("Fri")
	case time.Saturday:
		out.WriteString("Sat")
	}
}

// Appends the weekday as a number (1 .. 7)
func formatWeekdayNumber(d *fmtData, out *strings.Builder) {
	switch d.created.Weekday() {
	case time.Sunday:
		out.WriteRune('1')
	case time.Monday:
		out.WriteRune('2')
	case time.Tuesday:
		out.WriteRune('3')
	case time.Wednesday:
		out.WriteRune('4')
	case time.Thursday:
		out.WriteRune('5')
	case time.Friday:
		out.WriteRune('6')
	case time.Saturday:
		out.WriteRune('7')
	}
}

// Appends the weekday as a number (0 .. 6)
func formatWeekdayNumberZeroStart(d *fmtData, out *strings.Builder) {
	switch d.created.Weekday() {
	case time.Sunday:
		out.WriteRune('0')
	case time.Monday:
		out.WriteRune('1')
	case time.Tuesday:
		out.WriteRune('2')
	case time.Wednesday:
		out.WriteRune('3')
	case time.Thursday:
		out.WriteRune('4')
	case time.Friday:
		out.WriteRune('5')
	case time.Saturday:
		out.WriteRune('6')
	}
}

// Appends the year as a number.
func formatYear(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(d.created.Year()))
}

// Appends the year of the century (9 for 2009)
func formatYearOfCentury(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(d.created.Year() % 100))
}

// Appends the year of the century space padded ( 9 for 2009).
func formatYearOfCenturySpace(d *fmtData, out *strings.Builder) {
	year := d.created.Year() % 100
	if year < 10 {
		out.WriteRune(' ')
	}
	out.WriteString(strconv.Itoa(year))
}

// Appends the year of the century zero padded (09 for 2009)
func formatYearOfCenturyZero(d *fmtData, out *strings.Builder) {
	year := d.created.Year() % 100
	if year < 10 {
		out.WriteRune('0')
	}
	out.WriteString(strconv.Itoa(year))
}

// Appends the day of the month.
func formatYearDay(d *fmtData, out *strings.Builder) {
	out.WriteString(strconv.Itoa(d.created.YearDay()))
}

// Appends the day of the month zero padded.
func formatYearDaySpace(d *fmtData, out *strings.Builder) {
	day := d.created.YearDay()
	if day < 10 {
		out.WriteString("  ")
	} else if day < 100 {
		out.WriteRune(' ')
	}
	out.WriteString(strconv.Itoa(day))
}

// Appends the day of the month zero padded.
func formatYearDayZero(d *fmtData, out *strings.Builder) {
	day := d.created.YearDay()
	if day < 10 {
		out.WriteString("00")
	} else if day < 100 {
		out.WriteRune('0')
	}
	out.WriteString(strconv.Itoa(day))
}
