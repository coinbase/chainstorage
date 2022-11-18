package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	templateExt   = ".template.yml"
	baseTemplate  = "base.template.yml"
	localTemplate = "local.template.yml"
	devTemplate   = "development.template.yml"
	prodTemplate  = "production.template.yml"
)

var (
	baseTemplateRegexp  = regexp.MustCompile(`base(\..+)?\.template\.yml`)
	localTemplateRegexp = regexp.MustCompile(`local(\..+)?\.template\.yml`)
	devTemplateRegexp   = regexp.MustCompile(`development(\..+)?\.template\.yml`)
	prodTemplateRegexp  = regexp.MustCompile(`production(\..+)?\.template\.yml`)

	defaultAWSAccountMap = map[config.Env]config.AWSAccount{
		config.EnvDevelopment: config.AWSAccountDevelopment,
		config.EnvProduction:  config.AWSAccountProduction,
	}
)

type ConfigGenerator struct {
	chainStorageConfigPath string
	writePath              string
}

type ParentTemplates struct {
	base  []*ConfigTemplate
	local []*ConfigTemplate
	dev   []*ConfigTemplate
	prod  []*ConfigTemplate
}

// NewConfigGenerator returns a new ConfigGenerator with the specified
// Cloud and ChainStorage configuration paths
func NewConfigGenerator(chainStorageConfigPath string, writePath string) *ConfigGenerator {
	return &ConfigGenerator{
		chainStorageConfigPath: chainStorageConfigPath,
		writePath:              writePath,
	}
}

// Run generates YAML files from templates in the same directory
// for Cloud and ChainStorage configuration templates
func (c *ConfigGenerator) Run() error {
	err := c.generateChainStorageConfigs(c.chainStorageConfigPath)
	if err != nil {
		return xerrors.Errorf("failed generating ChainStorage configs: %w", err)
	}
	return nil
}

func (c *ConfigGenerator) generateChainStorageConfigs(configDir string) error {
	var parentTemplates ParentTemplates
	return c.walk(filepath.Dir(configDir), configDir, c.getEnvFromChainStorageConfig, &parentTemplates)
}

type walkEnvFunc func(path string) (*ConfigVars, error)

func (c *ConfigGenerator) addParentTemplate(parentTemplatePath string, fn walkEnvFunc, collection *[]*ConfigTemplate) error {
	_, err := os.Stat(parentTemplatePath)
	if err == nil {
		// only add parent templates as parents, ones without valid config environment scopes
		if _, err := fn(parentTemplatePath); err != nil {
			template, err := NewConfigTemplateFromFile(parentTemplatePath)
			if err != nil {
				return xerrors.Errorf("failed to read parent template [%s]: %w", parentTemplatePath, err)
			}
			*collection = append(*collection, template)
		}
	}
	return nil
}

func (c *ConfigGenerator) walk(seedPath string, path string, fn walkEnvFunc, parentTemplates *ParentTemplates) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return xerrors.Errorf("unable to read stat for path [%s]: %w", path, err)
	}

	if fileInfo.IsDir() {
		err = c.addParentTemplate(filepath.Join(path, baseTemplate), fn, &parentTemplates.base)
		if err != nil {
			return err
		}
		err = c.addParentTemplate(filepath.Join(path, localTemplate), fn, &parentTemplates.local)
		if err != nil {
			return err
		}
		err = c.addParentTemplate(filepath.Join(path, devTemplate), fn, &parentTemplates.dev)
		if err != nil {
			return err
		}
		err = c.addParentTemplate(filepath.Join(path, prodTemplate), fn, &parentTemplates.prod)
		if err != nil {
			return err
		}
		subDirs, err := os.ReadDir(path)
		if err != nil {
			return xerrors.Errorf("failed to read directory [%s]: %w", path, err)
		}
		for _, subDir := range subDirs {
			if err := c.walk(seedPath, filepath.Join(path, subDir.Name()), fn, parentTemplates); err != nil {
				return err
			}
		}
	} else if strings.HasSuffix(path, templateExt) {
		configVars, err := fn(path)
		if err != nil {
			return nil
		}
		baseFileName := filepath.Base(path)
		template, err := NewConfigTemplateFromFile(path)
		if err != nil {
			return xerrors.Errorf("unable to load config template [%s]: %w", path, err)
		}

		fileToWrite := filepath.Join(filepath.Dir(path), strings.TrimSuffix(baseFileName, templateExt)+".yml")
		fileToWrite = filepath.Join(c.writePath, strings.TrimPrefix(fileToWrite, seedPath))

		err = os.MkdirAll(filepath.Dir(fileToWrite), os.ModePerm)
		if err != nil {
			return xerrors.Errorf("failed to prepare directory for config file [%s]: %w", fileToWrite, err)
		}

		baseName := filepath.Base(path)
		environment := config.EnvDevelopment
		// default base config environment to `development`
		if baseTemplateRegexp.MatchString(baseName) {
			template.Inherit(parentTemplates.base...)
		} else if localTemplateRegexp.MatchString(baseName) {
			template.Inherit(parentTemplates.local...)
		} else if devTemplateRegexp.MatchString(baseName) {
			template.Inherit(parentTemplates.dev...)
		} else if prodTemplateRegexp.MatchString(baseName) {
			environment = config.EnvProduction
			template.Inherit(parentTemplates.prod...)
		}

		configVars.Environment = string(environment)
		configVars.AWSAccount = string(defaultAWSAccountMap[environment])

		err = template.WriteToFile(configVars, fileToWrite)
		if err != nil {
			return xerrors.Errorf("failed to write generated config for template [%s]: %w", path, err)
		}
	}
	return nil
}

func (c *ConfigGenerator) getEnvFromCloudConfig(path string) (*ConfigVars, error) {
	vals := strings.SplitN(filepath.Base(path), ".", 3)
	if len(vals) != 3 {
		return nil, xerrors.Errorf("unable to extract configName and Env from path [%s]", path)
	}
	env := vals[0]
	configName := vals[1]
	err := c.validateEnvironment(env)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract Env from path [%s]: %w", path, err)
	}
	configSplit := strings.Split(configName, "_")
	if len(configSplit) != 2 {
		return nil, xerrors.Errorf("unable to extract blockchain and network from config_name [%s]", configName)
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to extract configName from path [%s]: %w", path, err)
	}
	configVars := &ConfigVars{
		Blockchain:  configSplit[0],
		Network:     configSplit[1],
		Environment: env,
	}
	return configVars, nil
}

func (c *ConfigGenerator) getEnvFromChainStorageConfig(path string) (*ConfigVars, error) {
	env := strings.TrimSuffix(filepath.Base(path), templateExt)
	err := c.validateEnvironment(env)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract Env from path [%s]: %w", path, err)
	}

	dir, _ := filepath.Split(path)
	network := filepath.Base(dir)
	dir = filepath.Dir(filepath.Dir(dir))
	blockchain := filepath.Base(dir)

	err = c.validateBlockchainNetwork(blockchain, network)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract configName from path [%s]: %w", path, err)
	}
	configVars := &ConfigVars{
		Blockchain:  blockchain,
		Network:     network,
		Environment: env,
	}
	return configVars, nil
}

func (c *ConfigGenerator) validateEnvironment(envString string) error {
	configEnv := config.Env(envString)
	if configEnv == config.EnvLocal ||
		configEnv == config.EnvBase ||
		configEnv == config.EnvDevelopment ||
		configEnv == config.EnvProduction {
		return nil
	}
	return xerrors.Errorf("Invalid environment [%s]", envString)
}

func (c *ConfigGenerator) validateBlockchainNetwork(blockchain string, network string) error {
	asNetworkValue := "NETWORK_" + strings.ToUpper(blockchain+"_"+network)
	if _, ok := common.Network_value[asNetworkValue]; !ok {
		return xerrors.Errorf("Invalid blockchain [%s] or network [%s]", blockchain, network)
	}
	return nil
}
