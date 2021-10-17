package integration

import (
	"fmt"
	"os"
)

var (
	EnvMasterHost = "MASTER_HOST"
	EnvMasterPort = "MASTER_PORT"
	MasterHost    = os.Getenv(EnvMasterHost)
	MasterPort    = os.Getenv(EnvMasterPort)

	EnvSecondary1Host = "SECONDARY_1_HOST"
	EnvSecondary1Port = "SECONDARY_1_PORT"
	Secondary1Host    = os.Getenv(EnvSecondary1Host)
	Secondary1Port    = os.Getenv(EnvSecondary1Port)

	EnvSecondary2Host = "SECONDARY_2_HOST"
	EnvSecondary2Port = "SECONDARY_2_PORT"
	Secondary2Host    = os.Getenv(EnvSecondary2Host)
	Secondary2Port    = os.Getenv(EnvSecondary2Port)
)

type Config struct {
	Host string
	Port string
}

func (c Config) Address() string {
	return c.Host + ":" + c.Port
}

func MasterConfig() (Config, error) {
	if MasterHost == "" {
		return Config{}, fmt.Errorf("set %s", EnvMasterHost)
	}
	if MasterPort == "" {
		return Config{}, fmt.Errorf("set %s", EnvMasterPort)
	}

	return Config{
		Host: MasterHost,
		Port: MasterPort,
	}, nil
}

func Secondary1Config() (Config, error) {
	if Secondary1Host == "" {
		return Config{}, fmt.Errorf("set %s", EnvSecondary1Host)
	}
	if Secondary1Port == "" {
		return Config{}, fmt.Errorf("set %s", EnvSecondary1Port)
	}

	return Config{
		Host: Secondary1Host,
		Port: Secondary1Port,
	}, nil
}

func Secondary2Config() (Config, error) {
	if Secondary2Host == "" {
		return Config{}, fmt.Errorf("set %s", EnvSecondary2Host)
	}
	if Secondary2Port == "" {
		return Config{}, fmt.Errorf("set %s", EnvSecondary2Port)
	}

	return Config{
		Host: Secondary2Host,
		Port: Secondary2Port,
	}, nil
}
