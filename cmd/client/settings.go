package client

type ClientSettings struct {
	ConfigPath string
	BaseURL    string
	LogLevel string
}

var Settings ClientSettings