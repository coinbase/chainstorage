package fixtures

import (
	"encoding/json"
	"os"
	"path"
	"runtime"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/utils/finalizer"
)

func ReadFile(pathToFile string) ([]byte, error) {
	return FixturesFS.ReadFile(pathToFile)
}

func MustReadFile(pathToFile string) []byte {
	data, err := ReadFile(pathToFile)
	if err != nil {
		panic(err)
	}

	return data
}

func WriteFile(relPath string, data []byte) error {
	_, filename, _, _ := runtime.Caller(1)
	dir := path.Dir(filename)
	absPath := path.Join(dir, relPath)

	f, err := os.OpenFile(absPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	finalizer := finalizer.WithCloser(f)
	defer finalizer.Finalize()

	if _, err := f.Write(data); err != nil {
		return xerrors.Errorf("failed to write data: %w", err)
	}

	if _, err := f.WriteString("\n"); err != nil {
		return xerrors.Errorf("failed to write new line: %w", err)
	}

	return finalizer.Close()
}

func UnmarshalJSON(pathToFile string, out any) error {
	data, err := ReadFile(pathToFile)
	if err != nil {
		return xerrors.Errorf("failed to read file %v: %w", pathToFile, err)
	}

	if err := json.Unmarshal(data, out); err != nil {
		return xerrors.Errorf("failed to unmarshal file %v: %w", pathToFile, err)
	}

	return nil
}

func MustUnmarshalJSON(pathToFile string, out any) {
	if err := UnmarshalJSON(pathToFile, out); err != nil {
		panic(err)
	}
}

func MarshalJSON(pathToFile string, in any) error {
	data, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		return xerrors.Errorf("failed to marshal input: %w", err)
	}

	if err := WriteFile(pathToFile, data); err != nil {
		return xerrors.Errorf("failed to write to file: %w", err)
	}

	return nil
}

func MustMarshalJSON(pathToFile string, in any) {
	if err := MarshalJSON(pathToFile, in); err != nil {
		panic(err)
	}
}

func UnmarshalPB(pathToFile string, out proto.Message) error {
	data, err := ReadFile(pathToFile)
	if err != nil {
		return xerrors.Errorf("failed to read file %v: %w", pathToFile, err)
	}

	if err := protojson.Unmarshal(data, out); err != nil {
		return xerrors.Errorf("failed to unmarshal file %v: %w", pathToFile, err)
	}

	return nil
}

func MustUnmarshalPB(pathToFile string, out proto.Message) {
	if err := UnmarshalPB(pathToFile, out); err != nil {
		panic(err)
	}
}

func MarshalPB(pathToFile string, in proto.Message) error {
	marshaler := protojson.MarshalOptions{Indent: "  "}

	var buf []byte
	var err error
	if buf, err = marshaler.Marshal(in); err != nil {
		return xerrors.Errorf("failed to marshal input: %w", err)
	}

	if err := WriteFile(pathToFile, buf); err != nil {
		return xerrors.Errorf("failed to write to file: %w", err)
	}

	return nil
}

func MustMarshalPB(pathToFile string, in proto.Message) {
	if err := MarshalPB(pathToFile, in); err != nil {
		panic(err)
	}
}
