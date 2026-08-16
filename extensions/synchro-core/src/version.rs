use std::fmt;

/// A Semantic Versioning 2.0.0 version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Semver {
    pub major: String,
    pub minor: String,
    pub patch: String,
    prerelease: Vec<String>,
    build: Vec<String>,
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
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let (without_build, build) = split_build(s)?;
        let (core, prerelease) = match without_build.split_once('-') {
            Some((core, prerelease)) => (core, Some(prerelease)),
            None => (without_build, None),
        };
        let parts: Vec<&str> = core.split('.').collect();
        if parts.len() != 3 {
            return Err(parse_error(s, "expected major.minor.patch"));
        }

        let major = parse_core(parts[0], "major", s)?;
        let minor = parse_core(parts[1], "minor", s)?;
        let patch = parse_core(parts[2], "patch", s)?;
        let prerelease = parse_identifiers(prerelease, true, s)?;
        let build = parse_identifiers(build, false, s)?;

        Ok(Self {
            major,
            minor,
            patch,
            prerelease,
            build,
        })
    }

    /// Returns true if `self` is strictly less than `other`.
    pub fn less_than(&self, other: &Self) -> bool {
        self.cmp_precedence(other).is_lt()
    }

    pub fn prerelease(&self) -> &[String] {
        &self.prerelease
    }

    pub fn build(&self) -> &[String] {
        &self.build
    }

    fn cmp_precedence(&self, other: &Self) -> std::cmp::Ordering {
        for (left, right) in [
            (&self.major, &other.major),
            (&self.minor, &other.minor),
            (&self.patch, &other.patch),
        ] {
            let ordering = compare_numbers(left, right);
            if !ordering.is_eq() {
                return ordering;
            }
        }

        match (self.prerelease.as_slice(), other.prerelease.as_slice()) {
            ([], []) => std::cmp::Ordering::Equal,
            ([], _) => std::cmp::Ordering::Greater,
            (_, []) => std::cmp::Ordering::Less,
            (left, right) => compare_prerelease(left, right),
        }
    }
}

impl fmt::Display for Semver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if !self.prerelease.is_empty() {
            write!(f, "-{}", self.prerelease.join("."))?;
        }
        if !self.build.is_empty() {
            write!(f, "+{}", self.build.join("."))?;
        }
        Ok(())
    }
}

fn split_build(s: &str) -> Result<(&str, Option<&str>), ParseError> {
    match s.split_once('+') {
        Some((core, build)) if !build.contains('+') => Ok((core, Some(build))),
        Some(_) => Err(parse_error(s, "invalid build metadata")),
        None => Ok((s, None)),
    }
}

fn parse_core(s: &str, label: &str, input: &str) -> Result<String, ParseError> {
    if s.is_empty()
        || !s.bytes().all(|byte| byte.is_ascii_digit())
        || (s.len() > 1 && s.starts_with('0'))
    {
        return Err(parse_error(input, &format!("invalid {label}")));
    }
    Ok(s.into())
}

fn parse_identifiers(
    value: Option<&str>,
    prerelease: bool,
    input: &str,
) -> Result<Vec<String>, ParseError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let identifiers: Vec<&str> = value.split('.').collect();
    if identifiers.iter().any(|identifier| {
        identifier.is_empty()
            || !identifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || (prerelease
                && identifier.bytes().all(|byte| byte.is_ascii_digit())
                && identifier.len() > 1
                && identifier.starts_with('0'))
    }) {
        return Err(parse_error(input, "invalid identifier"));
    }
    Ok(identifiers.into_iter().map(str::to_owned).collect())
}

fn parse_error(input: &str, reason: &str) -> ParseError {
    ParseError {
        input: input.into(),
        reason: reason.into(),
    }
}

fn compare_numbers(left: &str, right: &str) -> std::cmp::Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn compare_prerelease(left: &[String], right: &[String]) -> std::cmp::Ordering {
    for (left, right) in left.iter().zip(right) {
        let left_numeric = left.bytes().all(|byte| byte.is_ascii_digit());
        let right_numeric = right.bytes().all(|byte| byte.is_ascii_digit());
        let ordering = match (left_numeric, right_numeric) {
            (true, true) => compare_numbers(left, right),
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => left.cmp(right),
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

/// Returns an error if `client_version` is strictly less than `min_version`.
///
/// Matches Go `CheckVersion`.
pub fn check_version(client_version: &str, min_version: &str) -> Result<(), VersionError> {
    let client = Semver::parse(client_version).map_err(|e| VersionError::Parse(e.to_string()))?;
    let min = Semver::parse(min_version).map_err(|e| VersionError::Parse(e.to_string()))?;

    if client.less_than(&min) {
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
    fn parse_and_display_valid_versions() {
        let v = Semver::parse("1.2.3").unwrap();
        assert_eq!(
            v,
            Semver {
                major: "1".into(),
                minor: "2".into(),
                patch: "3".into(),
                prerelease: Vec::new(),
                build: Vec::new(),
            }
        );
        assert_eq!(
            Semver::parse("1.2.3-beta.2+build.123").unwrap().to_string(),
            "1.2.3-beta.2+build.123"
        );
        assert_eq!(Semver::parse("1.2.3-0").unwrap().prerelease(), ["0"]);
        assert_eq!(Semver::parse("1.2.3+0").unwrap().build(), ["0"]);
        assert!(split_build("1.2.3+one+two").is_err());
    }

    #[test]
    fn rejects_invalid_syntax() {
        for input in [
            "v1.2.3",
            " 1.2.3",
            "1.2.3 ",
            "+1.2.3",
            "-1.2.3",
            "1.2",
            "1.2.3.4",
            "01.2.3",
            "1.02.3",
            "1.2.03",
            "1.2.3-01",
            "1.2.3-",
            "1.2.3+",
            "1.2.3-alpha..1",
            "1.2.3-β",
            "1.2.3+β",
            "1.2.3+one+two",
        ] {
            assert!(Semver::parse(input).is_err(), "{input}");
        }
    }

    #[test]
    fn applies_semver_precedence() {
        let pairs = [
            ("1.0.0-alpha", "1.0.0-alpha.1"),
            ("1.0.0-alpha.1", "1.0.0-alpha.beta"),
            ("1.0.0-alpha.beta", "1.0.0-beta"),
            ("1.0.0-beta", "1.0.0-beta.2"),
            ("1.0.0-beta.2", "1.0.0-beta.11"),
            ("1.0.0-beta.11", "1.0.0-rc.1"),
            ("1.0.0-rc.1", "1.0.0"),
            ("1.2.3-beta", "1.2.3"),
            ("1.2.3-beta.2", "1.2.3-beta.11"),
            (
                "999999999999999999999999.0.0",
                "1000000000000000000000000.0.0",
            ),
            (
                "1.0.0-999999999999999999999999",
                "1.0.0-1000000000000000000000000",
            ),
        ];
        for (lower, higher) in pairs {
            assert!(Semver::parse(lower)
                .unwrap()
                .less_than(&Semver::parse(higher).unwrap()));
        }
        assert!(!Semver::parse("1.2.3+one")
            .unwrap()
            .less_than(&Semver::parse("1.2.3+two").unwrap()));
        assert!(!Semver::parse("1.2.3+two")
            .unwrap()
            .less_than(&Semver::parse("1.2.3+one").unwrap()));
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
}
