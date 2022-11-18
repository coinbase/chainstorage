package fixtures

import (
	"bytes"
	"encoding/json"
	"os"
	"path"
	"runtime"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"

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

func UnmarshalJSON(pathToFile string, out interface{}) error {
	data, err := ReadFile(pathToFile)
	if err != nil {
		return xerrors.Errorf("failed to read file %v: %w", pathToFile, err)
	}

	if err := json.Unmarshal(data, out); err != nil {
		return xerrors.Errorf("failed to unmarshal file %v: %w", pathToFile, err)
	}

	return nil
}

func MustUnmarshalJSON(pathToFile string, out interface{}) {
	if err := UnmarshalJSON(pathToFile, out); err != nil {
		panic(err)
	}
}

func MarshalJSON(pathToFile string, in interface{}) error {
	data, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		return xerrors.Errorf("failed to marshal input: %w", err)
	}

	if err := WriteFile(pathToFile, data); err != nil {
		return xerrors.Errorf("failed to write to file: %w", err)
	}

	return nil
}

func MustMarshalJSON(pathToFile string, in interface{}) {
	if err := MarshalJSON(pathToFile, in); err != nil {
		panic(err)
	}
}

func UnmarshalPB(pathToFile string, out proto.Message) error {
	data, err := ReadFile(pathToFile)
	if err != nil {
		return xerrors.Errorf("failed to read file %v: %w", pathToFile, err)
	}

	if err := jsonpb.Unmarshal(bytes.NewReader(data), out); err != nil {
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
	marshaler := jsonpb.Marshaler{Indent: "  "}
	var buf bytes.Buffer
	if err := marshaler.Marshal(&buf, in); err != nil {
		return xerrors.Errorf("failed to marshal input: %w", err)
	}

	if err := WriteFile(pathToFile, buf.Bytes()); err != nil {
		return xerrors.Errorf("failed to write to file: %w", err)
	}

	return nil
}

func MustMarshalPB(pathToFile string, in proto.Message) {
	if err := MarshalPB(pathToFile, in); err != nil {
		panic(err)
	}
}
