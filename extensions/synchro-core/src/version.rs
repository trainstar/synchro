use std::fmt;

/// A parsed semantic version (major.minor.patch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Semver {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

/// Error returned when a version string cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub input: String,
    pub reason: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid semver {:?}: {}", self.input, self.reason)
    }
}

impl std::error::Error for ParseError {}

impl Semver {
    /// Parses a semver string. Strips leading 'v' and any pre-release suffix
    /// from the patch component (e.g., "1.2.3-beta" parses as 1.2.3).
    ///
    /// Matches Go `parseSemver` exactly.
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let s = s.strip_prefix('v').unwrap_or(s);

        let parts: Vec<&str> = s.splitn(3, '.').collect();
        if parts.len() != 3 {
            return Err(ParseError {
                input: s.into(),
                reason: "expected major.minor.patch".into(),
            });
        }

        let major = parse_component(parts[0], "major")?;
        let minor = parse_component(parts[1], "minor")?;

        // Strip pre-release / build metadata from patch.
        let patch_str = parts[2].split(&['-', '+'][..]).next().unwrap_or(parts[2]);
        let patch = parse_component(patch_str, "patch")?;

        Ok(Self {
            major,
            minor,
            patch,
        })
    }

    /// Returns true if `self` is strictly less than `other`.
    pub fn less_than(self, other: Self) -> bool {
        if self.major != other.major {
            return self.major < other.major;
        }
        if self.minor != other.minor {
            return self.minor < other.minor;
        }
        self.patch < other.patch
    }
}

impl fmt::Display for Semver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

fn parse_component(s: &str, label: &str) -> Result<u32, ParseError> {
    s.parse::<u32>().map_err(|_| ParseError {
        input: s.into(),
        reason: format!("invalid {label}"),
    })
}

/// Returns an error if `client_version` is strictly less than `min_version`.
///
/// Matches Go `CheckVersion`.
pub fn check_version(client_version: &str, min_version: &str) -> Result<(), VersionError> {
    let client = Semver::parse(client_version).map_err(|e| VersionError::Parse(e.to_string()))?;
    let min = Semver::parse(min_version).map_err(|e| VersionError::Parse(e.to_string()))?;

    if client.less_than(min) {
        Err(VersionError::UpgradeRequired)
    } else {
        Ok(())
    }
}

/// Errors from version checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionError {
    UpgradeRequired,
    Parse(String),
}

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UpgradeRequired => write!(f, "upgrade required"),
            Self::Parse(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for VersionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let v = Semver::parse("1.2.3").unwrap();
        assert_eq!(
            v,
            Semver {
                major: 1,
                minor: 2,
                patch: 3
            }
        );
    }

    #[test]
    fn parse_with_v_prefix() {
        let v = Semver::parse("v1.2.3").unwrap();
        assert_eq!(
            v,
            Semver {
                major: 1,
                minor: 2,
                patch: 3
            }
        );
    }

    #[test]
    fn parse_with_prerelease() {
        let v = Semver::parse("1.2.3-beta").unwrap();
        assert_eq!(
            v,
            Semver {
                major: 1,
                minor: 2,
                patch: 3
            }
        );
    }

    #[test]
    fn parse_with_build_metadata() {
        let v = Semver::parse("1.2.3+build.123").unwrap();
        assert_eq!(
            v,
            Semver {
                major: 1,
                minor: 2,
                patch: 3
            }
        );
    }

    #[test]
    fn parse_invalid_too_few_parts() {
        assert!(Semver::parse("1.2").is_err());
    }

    #[test]
    fn parse_invalid_non_numeric() {
        assert!(Semver::parse("a.b.c").is_err());
    }

    #[test]
    fn less_than_major() {
        assert!(Semver::parse("1.0.0")
            .unwrap()
            .less_than(Semver::parse("2.0.0").unwrap()));
        assert!(!Semver::parse("2.0.0")
            .unwrap()
            .less_than(Semver::parse("1.0.0").unwrap()));
    }

    #[test]
    fn less_than_minor() {
        assert!(Semver::parse("1.1.0")
            .unwrap()
            .less_than(Semver::parse("1.2.0").unwrap()));
        assert!(!Semver::parse("1.2.0")
            .unwrap()
            .less_than(Semver::parse("1.1.0").unwrap()));
    }

    #[test]
    fn less_than_patch() {
        assert!(Semver::parse("1.2.3")
            .unwrap()
            .less_than(Semver::parse("1.2.4").unwrap()));
        assert!(!Semver::parse("1.2.4")
            .unwrap()
            .less_than(Semver::parse("1.2.3").unwrap()));
    }

    #[test]
    fn less_than_equal() {
        assert!(!Semver::parse("1.2.3")
            .unwrap()
            .less_than(Semver::parse("1.2.3").unwrap()));
    }

    #[test]
    fn check_version_ok() {
        assert!(check_version("1.2.3", "1.2.3").is_ok());
        assert!(check_version("2.0.0", "1.2.3").is_ok());
    }

    #[test]
    fn check_version_upgrade_required() {
        assert_eq!(
            check_version("1.2.2", "1.2.3"),
            Err(VersionError::UpgradeRequired)
        );
    }

    #[test]
    fn display() {
        assert_eq!(Semver::parse("v1.2.3-beta").unwrap().to_string(), "1.2.3");
    }
}
