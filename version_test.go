package synchro

import (
	"errors"
	"testing"
)

func TestParseSemver(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    semver
		wantErr bool
	}{
		{
			name:  "simple version",
			input: "1.2.3",
			want:  semver{Major: 1, Minor: 2, Patch: 3},
		},
		{
			name:  "v prefix",
			input: "v2.0.1",
			want:  semver{Major: 2, Minor: 0, Patch: 1},
		},
		{
			name:  "pre-release suffix with dash",
			input: "1.0.0-beta",
			want:  semver{Major: 1, Minor: 0, Patch: 0},
		},
		{
			name:  "pre-release suffix with plus",
			input: "3.2.1+build.42",
			want:  semver{Major: 3, Minor: 2, Patch: 1},
		},
		{
			name:  "v prefix with pre-release",
			input: "v0.9.1-rc.1",
			want:  semver{Major: 0, Minor: 9, Patch: 1},
		},
		{
			name:  "zeros",
			input: "0.0.0",
			want:  semver{Major: 0, Minor: 0, Patch: 0},
		},
		{
			name:    "too few parts",
			input:   "1.2",
			wantErr: true,
		},
		{
			name:    "non-numeric major",
			input:   "abc.1.2",
			wantErr: true,
		},
		{
			name:    "non-numeric minor",
			input:   "1.abc.2",
			wantErr: true,
		},
		{
			name:    "non-numeric patch",
			input:   "1.2.abc",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSemver(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseSemver(%q) = %v, want error", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSemver(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("parseSemver(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSemver_LessThan(t *testing.T) {
	tests := []struct {
		name string
		a, b semver
		want bool
	}{
		{"major less", semver{1, 0, 0}, semver{2, 0, 0}, true},
		{"major greater", semver{2, 0, 0}, semver{1, 0, 0}, false},
		{"minor less", semver{1, 2, 0}, semver{1, 3, 0}, true},
		{"minor greater", semver{1, 3, 0}, semver{1, 2, 0}, false},
		{"patch less", semver{1, 2, 3}, semver{1, 2, 4}, true},
		{"patch greater", semver{1, 2, 4}, semver{1, 2, 3}, false},
		{"equal", semver{1, 2, 3}, semver{1, 2, 3}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.lessThan(tt.b); got != tt.want {
				t.Errorf("%v.lessThan(%v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestCheckVersion(t *testing.T) {
	tests := []struct {
		name          string
		clientVersion string
		minVersion    string
		wantErr       error
	}{
		{
			name:          "equal versions",
			clientVersion: "1.0.0",
			minVersion:    "1.0.0",
			wantErr:       nil,
		},
		{
			name:          "client newer major",
			clientVersion: "2.0.0",
			minVersion:    "1.0.0",
			wantErr:       nil,
		},
		{
			name:          "client newer minor",
			clientVersion: "1.5.0",
			minVersion:    "1.0.0",
			wantErr:       nil,
		},
		{
			name:          "client newer patch",
			clientVersion: "1.0.5",
			minVersion:    "1.0.0",
			wantErr:       nil,
		},
		{
			name:          "client older major",
			clientVersion: "1.0.0",
			minVersion:    "2.0.0",
			wantErr:       ErrUpgradeRequired,
		},
		{
			name:          "client older minor",
			clientVersion: "1.0.0",
			minVersion:    "1.1.0",
			wantErr:       ErrUpgradeRequired,
		},
		{
			name:          "client older patch",
			clientVersion: "1.0.0",
			minVersion:    "1.0.1",
			wantErr:       ErrUpgradeRequired,
		},
		{
			name:          "v prefix on both",
			clientVersion: "v1.0.0",
			minVersion:    "v2.0.0",
			wantErr:       ErrUpgradeRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckVersion(tt.clientVersion, tt.minVersion)
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("CheckVersion(%q, %q) = %v, want nil", tt.clientVersion, tt.minVersion, err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("CheckVersion(%q, %q) = %v, want %v", tt.clientVersion, tt.minVersion, err, tt.wantErr)
			}
		})
	}
}

func TestCheckVersion_InvalidVersions(t *testing.T) {
	tests := []struct {
		name          string
		clientVersion string
		minVersion    string
	}{
		{"invalid client", "bad", "1.0.0"},
		{"invalid min", "1.0.0", "bad"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckVersion(tt.clientVersion, tt.minVersion)
			if err == nil {
				t.Errorf("CheckVersion(%q, %q) = nil, want error", tt.clientVersion, tt.minVersion)
			}
			// Should not be ErrUpgradeRequired for parse errors.
			if errors.Is(err, ErrUpgradeRequired) {
				t.Errorf("expected parse error, got ErrUpgradeRequired")
			}
		})
	}
}
