package synchro

import (
	"fmt"
	"strconv"
	"strings"
)

// semver represents a parsed semantic version.
type semver struct {
	Major int
	Minor int
	Patch int
}

func parseSemver(s string) (semver, error) {
	// Strip leading 'v' if present
	s = strings.TrimPrefix(s, "v")

	parts := strings.SplitN(s, ".", 3)
	if len(parts) != 3 {
		return semver{}, fmt.Errorf("invalid semver: %q (expected major.minor.patch)", s)
	}

	// Strip any pre-release suffix from patch (e.g., "1-beta")
	patchStr := parts[2]
	if idx := strings.IndexAny(patchStr, "-+"); idx >= 0 {
		patchStr = patchStr[:idx]
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return semver{}, fmt.Errorf("invalid semver major: %q", parts[0])
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return semver{}, fmt.Errorf("invalid semver minor: %q", parts[1])
	}
	patch, err := strconv.Atoi(patchStr)
	if err != nil {
		return semver{}, fmt.Errorf("invalid semver patch: %q", patchStr)
	}

	return semver{Major: major, Minor: minor, Patch: patch}, nil
}

// lessThan returns true if v < other.
func (v semver) lessThan(other semver) bool {
	if v.Major != other.Major {
		return v.Major < other.Major
	}
	if v.Minor != other.Minor {
		return v.Minor < other.Minor
	}
	return v.Patch < other.Patch
}

// CheckVersion returns ErrUpgradeRequired if clientVersion < minVersion.
func CheckVersion(clientVersion, minVersion string) error {
	client, err := parseSemver(clientVersion)
	if err != nil {
		return fmt.Errorf("parsing client version: %w", err)
	}
	min, err := parseSemver(minVersion)
	if err != nil {
		return fmt.Errorf("parsing min version: %w", err)
	}

	if client.lessThan(min) {
		return ErrUpgradeRequired
	}
	return nil
}
