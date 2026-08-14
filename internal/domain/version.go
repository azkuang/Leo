package domain

import (
	"strconv"
	"strings"
)

// CompareVersions orders dotted numeric version strings numerically.
//
// This has to be numeric, not lexical: compute capability "10.0" is newer than
// "8.9" but sorts before it as text, and a lexical compare would silently
// exclude the newest hardware in the fleet from every job that asks for a
// floor. Driver ("580.95") and CUDA ("13.0") strings compare the same way.
//
// Returns -1 if a < b, 0 if equal, +1 if a > b. Missing components read as
// zero, so "13" and "13.0" are equal. Non-numeric trailing junk is ignored.
func CompareVersions(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	n := len(as)
	if len(bs) > n {
		n = len(bs)
	}
	for i := 0; i < n; i++ {
		if d := versionPart(as, i) - versionPart(bs, i); d != 0 {
			if d < 0 {
				return -1
			}
			return 1
		}
	}
	return 0
}

func versionPart(parts []string, i int) int {
	if i >= len(parts) {
		return 0
	}
	s := parts[i]
	// Trim any non-digit suffix, e.g. "535-server" -> "535".
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0
	}
	v, err := strconv.Atoi(s[:end])
	if err != nil {
		return 0
	}
	return v
}

// VersionAtLeast reports whether have satisfies the floor want. An empty floor
// is no constraint; an empty `have` fails any non-empty floor, because a node
// that cannot report its driver version cannot be assumed to meet one.
func VersionAtLeast(have, want string) bool {
	if want == "" {
		return true
	}
	if have == "" {
		return false
	}
	return CompareVersions(have, want) >= 0
}
