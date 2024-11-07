package internal

import (
	"fmt"

	"go.uber.org/fx"
)

type (
	ParserFactory interface {
		NewParser() (Parser, error)
	}

	ParserBuilder struct {
		name                 string
		nativeParserFactory  NativeParserFactory
		rosettaParserFactory RosettaParserFactory
		checkerFactory       CheckerFactory
		validatorFactory     ValidatorFactory
	}

	ParserFactoryOption  func(options any)
	NativeParserFactory  func(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error)
	RosettaParserFactory func(params ParserParams, nativeParser NativeParser, opts ...ParserFactoryOption) (RosettaParser, error)
	CheckerFactory       func(params ParserParams) (Checker, error)
	ValidatorFactory     func(params ParserParams) TrustlessValidator

	parserFactoryImpl struct {
		params               ParserParams
		nativeParserFactory  NativeParserFactory
		rosettaParserFactory RosettaParserFactory
		checkerFactory       CheckerFactory
		validatorFactory     ValidatorFactory
	}
)

func NewParserBuilder(name string, nativeParserFactory NativeParserFactory) *ParserBuilder {
	return &ParserBuilder{
		name:                 name,
		nativeParserFactory:  nativeParserFactory,
		rosettaParserFactory: NewNotImplementedRosettaParser,
		checkerFactory:       NewDefaultChecker,
		validatorFactory:     NewNotImplementedValidator,
	}
}

func (b *ParserBuilder) SetRosettaParserFactory(rosettaParserFactory RosettaParserFactory) *ParserBuilder {
	b.rosettaParserFactory = rosettaParserFactory
	return b
}

func (b *ParserBuilder) SetCheckerFactory(checkerFactory CheckerFactory) *ParserBuilder {
	b.checkerFactory = checkerFactory
	return b
}

func (b *ParserBuilder) SetValidatorFactory(validatorFactory ValidatorFactory) *ParserBuilder {
	b.validatorFactory = validatorFactory
	return b
}

func (b *ParserBuilder) Build() fx.Option {
	return fx.Provide(fx.Annotated{
		Name: b.name,
		Target: func(params ParserParams) ParserFactory {
			return &parserFactoryImpl{
				params:               params,
				nativeParserFactory:  b.nativeParserFactory,
				rosettaParserFactory: b.rosettaParserFactory,
				checkerFactory:       b.checkerFactory,
				validatorFactory:     b.validatorFactory,
			}
		},
	})
}

func (f *parserFactoryImpl) NewParser() (Parser, error) {
	nativeParser, err := f.nativeParserFactory(f.params)
	if err != nil {
		return nil, fmt.Errorf("failed to create native parser: %w", err)
	}

	rosettaParser, err := f.rosettaParserFactory(f.params, nativeParser)
	if err != nil {
		return nil, fmt.Errorf("failed to create rosetta parser: %w", err)
	}

	checker, err := f.checkerFactory(f.params)
	if err != nil {
		return nil, fmt.Errorf("failed to create checker: %w", err)
	}

	validator := f.validatorFactory(f.params)

	return &parserImpl{
		nativeParser:  nativeParser,
		rosettaParser: rosettaParser,
		checker:       checker,
		validator:     validator,
	}, nil
}
