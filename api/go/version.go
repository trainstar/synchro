package synchroapi

import (
	"fmt"
	"strings"
)

// semver represents a parsed semantic version.
type semver struct {
	major      string
	minor      string
	patch      string
	prerelease []semverIdentifier
}

type semverIdentifier struct {
	value   string
	numeric bool
}

func parseSemver(s string) (semver, error) {
	coreAndMetadata := strings.SplitN(s, "+", 2)
	if len(coreAndMetadata) == 2 {
		if !validSemverMetadata(coreAndMetadata[1], false) {
			return semver{}, fmt.Errorf("invalid semver build metadata: %q", coreAndMetadata[1])
		}
	}

	coreAndPrerelease := strings.SplitN(coreAndMetadata[0], "-", 2)
	core := strings.Split(coreAndPrerelease[0], ".")
	if len(core) != 3 {
		return semver{}, fmt.Errorf("invalid semver: %q (expected major.minor.patch)", s)
	}
	for _, component := range core {
		if !validSemverCoreNumber(component) {
			return semver{}, fmt.Errorf("invalid semver core component: %q", component)
		}
	}

	var prerelease []semverIdentifier
	if len(coreAndPrerelease) == 2 {
		if !validSemverMetadata(coreAndPrerelease[1], true) {
			return semver{}, fmt.Errorf("invalid semver prerelease: %q", coreAndPrerelease[1])
		}
		for _, identifier := range strings.Split(coreAndPrerelease[1], ".") {
			prerelease = append(prerelease, semverIdentifier{
				value:   identifier,
				numeric: isASCIIDigits(identifier),
			})
		}
	}

	return semver{
		major:      core[0],
		minor:      core[1],
		patch:      core[2],
		prerelease: prerelease,
	}, nil
}

func validSemverCoreNumber(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	return isASCIIDigits(value)
}

func validSemverMetadata(value string, prerelease bool) bool {
	if value == "" {
		return false
	}
	for _, identifier := range strings.Split(value, ".") {
		if identifier == "" {
			return false
		}
		if !validSemverIdentifier(identifier) {
			return false
		}
		if prerelease && isASCIIDigits(identifier) && len(identifier) > 1 && identifier[0] == '0' {
			return false
		}
	}
	return true
}

func validSemverIdentifier(value string) bool {
	for i := 0; i < len(value); i++ {
		char := value[i]
		if (char < '0' || char > '9') &&
			(char < 'A' || char > 'Z') &&
			(char < 'a' || char > 'z') && char != '-' {
			return false
		}
	}
	return true
}

func isASCIIDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

// lessThan returns true if v < other.
func (v semver) lessThan(other semver) bool {
	if comparison := compareSemverCoreNumber(v.major, other.major); comparison != 0 {
		return comparison < 0
	}
	if comparison := compareSemverCoreNumber(v.minor, other.minor); comparison != 0 {
		return comparison < 0
	}
	if comparison := compareSemverCoreNumber(v.patch, other.patch); comparison != 0 {
		return comparison < 0
	}
	if len(v.prerelease) == 0 || len(other.prerelease) == 0 {
		return len(v.prerelease) > 0 && len(other.prerelease) == 0
	}
	for i := 0; i < len(v.prerelease) && i < len(other.prerelease); i++ {
		left, right := v.prerelease[i], other.prerelease[i]
		if left.numeric && right.numeric {
			if comparison := compareSemverCoreNumber(left.value, right.value); comparison != 0 {
				return comparison < 0
			}
			continue
		}
		if left.numeric != right.numeric {
			return left.numeric
		}
		if left.value != right.value {
			return left.value < right.value
		}
	}
	return len(v.prerelease) < len(other.prerelease)
}

func compareSemverCoreNumber(left, right string) int {
	if len(left) != len(right) {
		if len(left) < len(right) {
			return -1
		}
		return 1
	}
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

// checkVersion returns a non-nil error if clientVersion < minVersion.
func checkVersion(clientVersion, minVersion string) error {
	client, err := parseSemver(clientVersion)
	if err != nil {
		return fmt.Errorf("parsing client version: %w", err)
	}
	min, err := parseSemver(minVersion)
	if err != nil {
		return fmt.Errorf("parsing min version: %w", err)
	}

	if client.lessThan(min) {
		return errUpgradeRequired
	}
	return nil
}

var errUpgradeRequired = fmt.Errorf("client upgrade required")
