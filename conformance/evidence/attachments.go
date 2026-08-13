package evidence

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/trainstar/synchro/conformance/execution"
)

const attachmentDirectory = "attachments"

var attachmentKinds = map[string]struct{}{
	"log":                      {},
	"trace":                    {},
	"replay-data":              {},
	"barrier-trace":            {},
	"fault-plan":               {},
	"negative-control":         {},
	"performance-measurements": {},
	"vector-results":           {},
	"report":                   {},
}

// Put publishes one private content-addressed attachment without replacing an
// existing final file. A matching final file is a duplicate and returns an
// error after verification.
func (s *Store) Put(kind, mediaType string, data []byte) (Attachment, error) {
	attachment, name, err := attachmentFor(kind, mediaType, data)
	if err != nil {
		return Attachment{}, err
	}
	root, attachments, err := s.openAttachmentRoot(true)
	if err != nil {
		return Attachment{}, err
	}
	defer root.Close()
	defer attachments.Close()

	temporary, file, err := createPrivateTemporary(attachments)
	if err != nil {
		return Attachment{}, err
	}
	keepTemporary := true
	defer func() {
		_ = file.Close()
		if keepTemporary {
			_ = attachments.Remove(temporary)
			_ = syncDirectory(attachments)
		}
	}()
	if err := writeAll(file, data); err != nil {
		return Attachment{}, fmt.Errorf("%w: write private temporary: %v", ErrInvalidAttachment, err)
	}
	if err := file.Sync(); err != nil {
		return Attachment{}, fmt.Errorf("%w: fsync private temporary: %v", ErrInvalidAttachment, err)
	}
	if err := verifyOpenedRegular(attachments, temporary, file, int64(len(data)), attachment.SHA256); err != nil {
		return Attachment{}, err
	}
	if err := file.Close(); err != nil {
		return Attachment{}, fmt.Errorf("%w: close private temporary: %v", ErrInvalidAttachment, err)
	}

	if err := attachments.Link(temporary, name); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return Attachment{}, fmt.Errorf("%w: exclusive attachment publication: %v", ErrInvalidAttachment, err)
		}
		if verifyErr := verifyStoredAttachment(attachments, name, attachment); verifyErr != nil {
			return Attachment{}, verifyErr
		}
		if removeErr := attachments.Remove(temporary); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			return Attachment{}, fmt.Errorf("%w: remove duplicate temporary: %v", ErrInvalidAttachment, removeErr)
		}
		keepTemporary = false
		if syncErr := syncDirectory(attachments); syncErr != nil {
			return Attachment{}, syncErr
		}
		return attachment, ErrDuplicateAttachment
	}
	if err := verifyStoredAttachment(attachments, name, attachment); err != nil {
		return Attachment{}, err
	}
	if err := syncDirectory(attachments); err != nil {
		return Attachment{}, err
	}
	if err := attachments.Remove(temporary); err != nil {
		return Attachment{}, fmt.Errorf("%w: remove published temporary: %v", ErrInvalidAttachment, err)
	}
	keepTemporary = false
	if err := syncDirectory(attachments); err != nil {
		return Attachment{}, err
	}
	return attachment, nil
}

// Publish stores one runner attachment and returns its receipt projection.
func (s *Store) Publish(kind, mediaType string, data []byte) (execution.Attachment, error) {
	attachment, err := s.Put(kind, mediaType, data)
	if err != nil {
		return execution.Attachment{}, err
	}
	return execution.Attachment{
		ID:        attachment.ID,
		Kind:      attachment.Kind,
		Path:      attachment.Path,
		MediaType: attachment.MediaType,
		SizeBytes: attachment.SizeBytes,
		SHA256:    attachment.SHA256,
	}, nil
}

// Verify checks an attachment against its content-addressed candidate path.
func (s *Store) Verify(attachment Attachment) error {
	name, err := attachmentFileName(attachment)
	if err != nil {
		return err
	}
	root, attachments, err := s.openAttachmentRoot(false)
	if err != nil {
		return err
	}
	defer root.Close()
	defer attachments.Close()
	if err := verifyStoredAttachment(attachments, name, attachment); err != nil {
		return err
	}
	if err := s.verifyRootIdentity(); err != nil {
		return err
	}
	return nil
}

func (s *Store) openAttachmentRoot(create bool) (*os.Root, *os.Root, error) {
	if s == nil || s.Root == "" {
		return nil, nil, ErrInvalidStore
	}
	root, _, err := openCandidateRoot(s.Root, s.rootIdentity)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: open candidate root: %v", ErrInvalidStore, err)
	}
	closeRoot := true
	defer func() {
		if closeRoot {
			_ = root.Close()
		}
	}()
	info, err := root.Lstat(attachmentDirectory)
	if errors.Is(err, fs.ErrNotExist) {
		if !create {
			return nil, nil, fmt.Errorf("%w: attachment directory is missing", ErrInvalidAttachment)
		}
		if err := root.Mkdir(attachmentDirectory, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return nil, nil, fmt.Errorf("%w: create attachment directory: %v", ErrInvalidStore, err)
		}
		info, err = root.Lstat(attachmentDirectory)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("%w: inspect attachment directory: %v", ErrInvalidStore, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, nil, fmt.Errorf("%w: attachment directory is unsafe", ErrInvalidStore)
	}
	attachments, err := root.OpenRoot(attachmentDirectory)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: open attachment directory: %v", ErrInvalidStore, err)
	}
	opened, err := attachments.Stat(".")
	if err != nil {
		_ = attachments.Close()
		return nil, nil, fmt.Errorf("%w: inspect opened attachment directory: %v", ErrInvalidStore, err)
	}
	current, err := root.Lstat(attachmentDirectory)
	if err != nil || current.Mode()&os.ModeSymlink != 0 || !current.IsDir() || !os.SameFile(opened, current) {
		_ = attachments.Close()
		return nil, nil, fmt.Errorf("%w: attachment directory identity changed", ErrInvalidStore)
	}
	closeRoot = false
	return root, attachments, nil
}

func (s *Store) verifyRootIdentity() error {
	if s == nil {
		return ErrInvalidStore
	}
	root, _, err := openCandidateRoot(s.Root, s.rootIdentity)
	if err != nil {
		return fmt.Errorf("%w: candidate root identity: %v", ErrInvalidStore, err)
	}
	return root.Close()
}

func candidateRootPath(root string) (string, error) {
	if strings.IndexByte(root, 0) >= 0 {
		return "", fmt.Errorf("%w: candidate root contains NUL", ErrInvalidStore)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("%w: resolve candidate root: %v", ErrInvalidStore, err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return "", fmt.Errorf("%w: inspect candidate root: %v", ErrInvalidStore, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("%w: candidate root is not a non-symlink directory", ErrInvalidStore)
	}
	return filepath.Clean(abs), nil
}

func attachmentFor(kind, mediaType string, data []byte) (Attachment, string, error) {
	if _, found := attachmentKinds[kind]; !found || mediaType == "" || len(mediaType) > 256 || strings.ContainsAny(mediaType, "\r\n\x00") {
		return Attachment{}, "", fmt.Errorf("%w: attachment kind or media type", ErrInvalidAttachment)
	}
	digest := sha256.Sum256(data)
	encoded := hex.EncodeToString(digest[:])
	identifier := "ATT-" + strings.ToUpper(strings.ReplaceAll(kind, "-", "-")) + "-" + strings.ToUpper(encoded) + "-001"
	attachment := Attachment{
		ID:        identifier,
		Kind:      kind,
		Path:      attachmentDirectory + "/" + kind + "-sha256-" + encoded + ".bin",
		MediaType: mediaType,
		SizeBytes: int64(len(data)),
		SHA256:    encoded,
	}
	name, err := attachmentFileName(attachment)
	if err != nil {
		return Attachment{}, "", err
	}
	return attachment, name, nil
}

func attachmentFileName(attachment Attachment) (string, error) {
	if _, found := attachmentKinds[attachment.Kind]; !found || attachment.MediaType == "" || attachment.SizeBytes < 0 || !validSHA256(attachment.SHA256) {
		return "", fmt.Errorf("%w: attachment metadata", ErrInvalidAttachment)
	}
	name := attachment.Kind + "-sha256-" + attachment.SHA256 + ".bin"
	wantPath := attachmentDirectory + "/" + name
	wantID := "ATT-" + strings.ToUpper(attachment.Kind) + "-" + strings.ToUpper(attachment.SHA256) + "-001"
	if attachment.ID != wantID || attachment.Path != wantPath {
		return "", fmt.Errorf("%w: attachment path or ID is not content addressed", ErrInvalidAttachment)
	}
	if strings.ContainsAny(name, "/\\\x00") {
		return "", fmt.Errorf("%w: attachment file name", ErrInvalidAttachment)
	}
	return name, nil
}

func createPrivateTemporary(root *os.Root) (string, *os.File, error) {
	for attempt := 0; attempt < 32; attempt++ {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return "", nil, fmt.Errorf("%w: random temporary name: %v", ErrInvalidAttachment, err)
		}
		name := ".private-tmp-" + hex.EncodeToString(nonce[:])
		file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			info, infoErr := root.Lstat(name)
			if infoErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
				_ = file.Close()
				_ = root.Remove(name)
				return "", nil, fmt.Errorf("%w: temporary file is unsafe", ErrInvalidAttachment)
			}
			return name, file, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", nil, fmt.Errorf("%w: create private temporary: %v", ErrInvalidAttachment, err)
		}
	}
	return "", nil, fmt.Errorf("%w: could not allocate private temporary", ErrInvalidAttachment)
}

func verifyStoredAttachment(root *os.Root, name string, attachment Attachment) error {
	file, err := root.Open(name)
	if err != nil {
		return fmt.Errorf("%w: open attachment: %v", ErrInvalidAttachment, err)
	}
	defer file.Close()
	if err := verifyOpenedRegular(root, name, file, attachment.SizeBytes, attachment.SHA256); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("%w: fsync published attachment: %v", ErrInvalidAttachment, err)
	}
	return nil
}

func verifyOpenedRegular(root *os.Root, name string, file *os.File, size int64, digest string) error {
	if root == nil || file == nil || size < 0 || !validSHA256(digest) {
		return fmt.Errorf("%w: attachment verification input", ErrInvalidAttachment)
	}
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("%w: stat opened attachment: %v", ErrInvalidAttachment, err)
	}
	current, err := root.Lstat(name)
	if err != nil {
		return fmt.Errorf("%w: lstat attachment: %v", ErrInvalidAttachment, err)
	}
	if current.Mode()&os.ModeSymlink != 0 || !opened.Mode().IsRegular() || !current.Mode().IsRegular() || opened.Mode().Perm()&0o077 != 0 || !os.SameFile(opened, current) || opened.Size() != size {
		return fmt.Errorf("%w: attachment file identity", ErrInvalidAttachment)
	}
	hash := sha256.New()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("%w: seek attachment: %v", ErrInvalidAttachment, err)
	}
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("%w: hash attachment: %v", ErrInvalidAttachment, err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != digest {
		return fmt.Errorf("%w: attachment bytes changed", ErrInvalidAttachment)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("%w: rewind attachment: %v", ErrInvalidAttachment, err)
	}
	second, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("%w: reread attachment: %v", ErrInvalidAttachment, err)
	}
	secondDigest := sha256.Sum256(second)
	if int64(len(second)) != size || hex.EncodeToString(secondDigest[:]) != digest {
		return fmt.Errorf("%w: attachment changed during verification", ErrInvalidAttachment)
	}
	return nil
}

func syncDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("%w: open attachment directory for fsync: %v", ErrInvalidAttachment, err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("%w: fsync attachment directory: %v", ErrInvalidAttachment, err)
	}
	return nil
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		count, err := file.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && value == hex.EncodeToString(decoded)
}

func attachmentBytesEqual(left, right []byte) bool {
	return bytes.Equal(left, right)
}
