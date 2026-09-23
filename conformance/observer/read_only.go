package observer

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var mutationWords = map[string]struct{}{
	"alter": {}, "analyze": {}, "call": {}, "comment": {}, "commit": {}, "copy": {},
	"create": {}, "deallocate": {}, "delete": {}, "discard": {}, "do": {}, "drop": {},
	"execute": {}, "grant": {}, "insert": {}, "listen": {}, "lock": {}, "merge": {},
	"notify": {}, "prepare": {}, "refresh": {}, "reset": {}, "revoke": {}, "rollback": {},
	"set": {}, "truncate": {}, "update": {}, "vacuum": {},
}

// ValidateReadOnlySQL rejects write-capable SQL and direct extension-internal table access.
func ValidateReadOnlySQL(statement string) error {
	words, err := SQLWords(statement)
	if err != nil {
		return err
	}
	if len(words) == 0 || (words[0] != "select" && words[0] != "with" && words[0] != "explain") {
		return errors.New("observer SQL must begin with SELECT, WITH, or EXPLAIN")
	}
	for index, word := range words {
		if _, forbidden := mutationWords[word]; forbidden {
			return fmt.Errorf("observer SQL contains a mutation verb")
		}
		if strings.HasPrefix(word, "sync_") {
			return errors.New("observer SQL accesses an internal sync table")
		}
		if word == "into" && index > 0 {
			return errors.New("observer SQL contains SELECT INTO")
		}
	}
	return nil
}

// SQLWords returns lower-case SQL tokens while ignoring quoted text and comments.
func SQLWords(statement string) ([]string, error) {
	var words []string
	for index := 0; index < len(statement); {
		character := rune(statement[index])
		switch {
		case unicode.IsSpace(character) || strings.ContainsRune("(),.;", character):
			index++
		case character == '-' && index+1 < len(statement) && statement[index+1] == '-':
			index += 2
			for index < len(statement) && statement[index] != '\n' {
				index++
			}
		case character == '/' && index+1 < len(statement) && statement[index+1] == '*':
			end := strings.Index(statement[index+2:], "*/")
			if end < 0 {
				return nil, errors.New("observer SQL has an unterminated comment")
			}
			index += end + 4
		case character == '\'':
			quote := byte(character)
			index++
			closed := false
			for index < len(statement) {
				if statement[index] == quote {
					if index+1 < len(statement) && statement[index+1] == quote {
						index += 2
						continue
					}
					index++
					closed = true
					break
				}
				index++
			}
			if !closed {
				return nil, errors.New("observer SQL has an unterminated quoted value")
			}
		case character == '"':
			index++
			var identifier strings.Builder
			closed := false
			for index < len(statement) {
				if statement[index] == '"' {
					if index+1 < len(statement) && statement[index+1] == '"' {
						identifier.WriteByte('"')
						index += 2
						continue
					}
					index++
					closed = true
					break
				}
				identifier.WriteByte(statement[index])
				index++
			}
			if !closed {
				return nil, errors.New("observer SQL has an unterminated quoted identifier")
			}
			if identifier.Len() != 0 {
				words = append(words, strings.ToLower(identifier.String()))
			}
		case isIdentifierStart(byte(character)):
			start := index
			index++
			for index < len(statement) && isIdentifierPart(statement[index]) {
				index++
			}
			words = append(words, strings.ToLower(statement[start:index]))
		default:
			index++
		}
	}
	return words, nil
}

func isIdentifierStart(value byte) bool {
	return value == '_' || (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z')
}

func isIdentifierPart(value byte) bool {
	return isIdentifierStart(value) || (value >= '0' && value <= '9')
}

func validQualifiedIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for _, part := range strings.Split(value, ".") {
		if part == "" || !isIdentifierStart(part[0]) {
			return false
		}
		for index := 1; index < len(part); index++ {
			if !isIdentifierPart(part[index]) {
				return false
			}
		}
		if strings.HasPrefix(strings.ToLower(part), "sync_") {
			return false
		}
	}
	return true
}
