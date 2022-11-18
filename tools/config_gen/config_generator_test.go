package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestConfigGenerator(t *testing.T) {
	require := testutil.Require(t)

	tempDir, err := ioutil.TempDir("", "chainstorage_config_gen_test")
	require.Nil(err)
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	templatesDir, err := setupTemplatesDirFromFixtures(tempDir)
	require.Nil(err)

	configsDir := filepath.Join(tempDir, "expected_configs")
	err = os.Mkdir(configsDir, os.ModePerm)
	require.Nil(err)

	gen := NewConfigGenerator(filepath.Join(templatesDir, "config"), configsDir)
	require.Nil(gen.Run())

	compareConfigs(require, "tools/config_gen/expected_configs/config/chainstorage/ethereum/mainnet/base.yml", filepath.Join(configsDir, "config", "chainstorage", "ethereum", "mainnet", "base.yml"))
	compareConfigs(require, "tools/config_gen/expected_configs/config/chainstorage/ethereum/mainnet/development.yml", filepath.Join(configsDir, "config", "chainstorage", "ethereum", "mainnet", "development.yml"))
	compareConfigs(require, "tools/config_gen/expected_configs/config/chainstorage/ethereum/mainnet/production.yml", filepath.Join(configsDir, "config", "chainstorage", "ethereum", "mainnet", "production.yml"))
}

func compareConfigs(require *testutil.Assertions, expectedFixtureFile string, actualConfigFile string) {
	expectedCloudConfig, err := fixtures.ReadFile(expectedFixtureFile)
	require.Nil(err)
	actualCloudConfig, err := ioutil.ReadFile(actualConfigFile)
	require.Nil(err)
	require.Equal(string(expectedCloudConfig), string(actualCloudConfig))
}

func setupTemplatesDirFromFixtures(rootPath string) (string, error) {
	templatesDir := filepath.Join(rootPath, "config_templates")
	err := os.Mkdir(templatesDir, os.ModePerm)
	if err != nil {
		return "", err
	}

	// make config dir
	err = os.MkdirAll(filepath.Join(templatesDir, "config", "chainstorage", "ethereum", "mainnet"), os.ModePerm)
	if err != nil {
		return "", err
	}
	commonConfigDat, err := fixtures.ReadFile("tools/config_gen/config_templates/config/base.template.yml")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(filepath.Join(templatesDir, "config", "base.template.yml"), commonConfigDat, 0644)
	if err != nil {
		return "", err
	}
	commonDevConfigDat, err := fixtures.ReadFile("tools/config_gen/config_templates/config/development.template.yml")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(filepath.Join(templatesDir, "config", "development.template.yml"), commonDevConfigDat, 0644)
	if err != nil {
		return "", err
	}
	commonProdConfigDat, err := fixtures.ReadFile("tools/config_gen/config_templates/config/production.template.yml")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(filepath.Join(templatesDir, "config", "production.template.yml"), commonProdConfigDat, 0644)
	if err != nil {
		return "", err
	}
	baseConfigDat, err := fixtures.ReadFile("tools/config_gen/config_templates/config/chainstorage/ethereum/mainnet/base.template.yml")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(filepath.Join(templatesDir, "config", "chainstorage", "ethereum", "mainnet", "base.template.yml"), baseConfigDat, 0644)
	if err != nil {
		return "", err
	}
	devConfigDat, err := fixtures.ReadFile("tools/config_gen/config_templates/config/chainstorage/ethereum/mainnet/development.template.yml")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(filepath.Join(templatesDir, "config", "chainstorage", "ethereum", "mainnet", "development.template.yml"), devConfigDat, 0644)
	if err != nil {
		return "", err
	}
	prodConfigDat, err := fixtures.ReadFile("tools/config_gen/config_templates/config/chainstorage/ethereum/mainnet/production.template.yml")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(filepath.Join(templatesDir, "config", "chainstorage", "ethereum", "mainnet", "production.template.yml"), prodConfigDat, 0644)
	if err != nil {
		return "", err
	}

	return templatesDir, nil
}
