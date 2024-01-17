package config

import (
	"fmt"
)

// Validates that the given string is valid for use as a domain in a cookie.
// Valid domains always start with an alphanumeric, can include a dash or
// a period but can not have them repeated, and may end with a period at
// the end of the string.
func isValidCookieDomain(d string) bool {
	if len(d) == 0 {
		return false
	}
	var last rune
	for _, r := range d {
		switch {
		case r >= 'a' && r <= 'z':
			fallthrough
		case r >= 'A' && r <= 'Z':
			fallthrough
		case r >= '0' && r <= '9':
		case r == '-':
			if last == 0 || last == '-' || last == '.' {
				return false
			}
		case r == '.':
			if last == '-' || last == '.' {
				return false
			}
		}
		last = r
	}
	if last == '-' {
		return false
	}
	return true
}

// Returns true if the string given is valid for use as a cookie name. Cookie
// names can use any US-ASCII character except control characters and some
// limited special characters.
func isValidCookieName(s string) bool {
	for _, r := range s {
		switch r {
		case '[', ']':
		case '{', '}':
		case '(', ')':
		case '<', '>':
		case '@', ',', ';', ':', '\\', '"', '/', '?', '=':
		case ' ', '\t':
		default:
			if r > 32 && r < 127 {
				continue
			}
		}
		return false
	}
	return len(s) > 0
}

// Checks to see if any clue in a list of strings is duplicated. This returns
// error results that can be included in the validation results.
func hasDuplicates(name string, list []string) (errors []string) {
	seen := make(map[string]bool, len(list))
	for i, s := range list {
		if errored, ok := seen[s]; !ok {
			seen[s] = false
		} else if !errored {
			errors = append(errors, fmt.Sprintf(
				"%s: Duplicate item in the list at index %d: %s",
				name,
				i,
				s))
			seen[s] = true
		}
	}
	return
}

// Returns an error if any of the items in the list are empty ("").
func hasEmpty(name string, list []string) (errors []string) {
	for i, s := range list {
		if s == "" {
			errors = append(errors, fmt.Sprintf(
				"%s: List contains an empty string at index %d",
				name,
				i,
			))
		}
	}
	return
}
