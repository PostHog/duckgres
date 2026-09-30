//go:build kubernetes

package controlplane

import (
	"errors"
	"io"
	"os"
	"unicode/utf8"
)

func readTrinoPoolSecretFile(name string, limit int64) ([]byte, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, errors.New("pool secret file unavailable")
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit || !utf8.Valid(data) {
		return nil, errors.New("invalid pool secret file")
	}
	return data, nil
}
