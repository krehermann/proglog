package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile         = configFile("model.conf")
	ACLPolicyFile        = configFile("policy.csv")
	DefaultConfigDir     = ".proglog"
)

func configFile(filename string) string {
	dir := os.Getenv("PROLOG_CONFIG_DIR")
	if dir == "" {
		//var err error
		homeDir, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		dir = filepath.Join(homeDir, DefaultConfigDir)
	}
	return filepath.Join(dir, filename)
}
