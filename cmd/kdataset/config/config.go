package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"

	"github.com/Sirupsen/logrus"
)

var Config *DealerConfig

type DealerConfig struct {
	BaseURL string `yaml:"base_url"`
	PlukURL string `yaml:"pluk_url"`
	Token   string `yaml:"token"`
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

// Load reads data, deserializes it as KruegerConfig and assign as the global Config.
func Load(data []byte) error {
	cfg := DealerConfig{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	Config = &cfg
	return nil
}
