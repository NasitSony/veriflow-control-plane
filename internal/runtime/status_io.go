package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func WriteRunStatus(path string, s *RunStatus) error {
	if s == nil {
		return fmt.Errorf("run status is nil")
	}
	if path == "" {
		return fmt.Errorf("status path is empty")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create status dir: %w", err)
	}

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run status: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write temp status file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp status file: %w", err)
	}

	return nil
}

func ReadRunStatus(path string) (*RunStatus, error) {
	if path == "" {
		return nil, fmt.Errorf("status path is empty")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("read status file: %w", err)
	}

	var s RunStatus
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("unmarshal run status: %w", err)
	}

	return &s, nil
}
