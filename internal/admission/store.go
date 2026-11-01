package admission

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const MaxConfigBytes = 2 << 20

func DecodeConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > MaxConfigBytes {
		return c, fmt.Errorf("configuration exceeds 2 MiB")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, fmt.Errorf("expected one JSON document")
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func Load(path string, fallback Config) (Config, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return fallback.Clone(), nil
	}
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, MaxConfigBytes+1))
	if err != nil {
		return Config{}, err
	}
	return DecodeConfig(raw)
}

// Save commits one validated configuration using a same-directory temporary
// file. No in-memory policy is changed until this operation succeeds.
func Save(path string, c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if len(raw) > MaxConfigBytes {
		return fmt.Errorf("configuration exceeds 2 MiB")
	}
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".queue-config-*.tmp")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(append(raw, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(name, path)
}
