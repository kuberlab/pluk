package config

import (
	"io/ioutil"
	"os"

	"github.com/Sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

var Config *DealerConfig

type DealerConfig struct {
	BaseURL         string `yaml:"base_url"`
	PlukURL         string `yaml:"pluk_url"`
	Token           string `yaml:"token"`
	Workspace       string `yaml:"workspace"`
	WorkspaceSecret string `yaml:"workspace_secret"`
	InternalKey     string `yaml:"internal_key"`
	Insecure        bool   `yaml:"insecure"`
}

func InitConfigField(field *string, cliValue, envVarName, defaultValue string) {
	// 1. CLI value
	if cliValue != "" {
		*field = cliValue
		return
	}
	// 2. Env value
	envValue := os.Getenv(envVarName)
	if envValue != "" {
		*field = envValue
		return
	}
	// 3. Default value if not set
	if *field == "" {
		*field = defaultValue
	}
}

// InitConfig loads Config from the given path.
func InitConfig(filepath string) error {
	data, err := ioutil.ReadFile(filepath)

	if err != nil {
		return err
	}

	if err := Load(data); err != nil {
		return err
	}

	logrus.Debugf("Config loaded from %v.", filepath)
	return nil
}

// Load reads data, deserialize it as DealerConfig and assign as the global Config.
func Load(data []byte) error {
	cfg := DealerConfig{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	Config = &cfg
	return nil
}
