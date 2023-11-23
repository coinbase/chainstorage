package chainstorage

import (
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoimpl"
)

const Delimiter = "_"

func (x SolanaProgram) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return strings.ToLower(strings.Replace(str, Delimiter, "-", -1))
}

func (x SolanaAddressLookupTableProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaBpfLoaderProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaBpfUpgradeableLoaderProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaVoteProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaSystemProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaStakeProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaSplMemoProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaSplTokenProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaSplToken2022Program_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func (x SolanaSplAssociatedTokenAccountProgram_InstructionType) Name() string {
	str := protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
	return toCammelCase(str)
}

func toCammelCase(str string) string {
	tokens := strings.Split(str, Delimiter)

	cammelCaseName := ""
	for i, token := range tokens {
		word := token
		if i == 0 {
			if token != "" {
				word = strings.ToLower(token)
			}
		} else {
			if token != "" {
				word = cases.Title(language.English, cases.Compact).String(strings.ToLower(token))
			}
		}
		cammelCaseName += word
	}

	return cammelCaseName
}
