// Package argument is used to read command line arguments of the application.
package argument

import (
	"fmt"
	"os"
	"strings"

	"github.com/Seascape-Foundation/sds-service-lib/log"
)

const (
	SECURE        = "secure" // If passed, then TCP sockets will require authentication. Default is false
	SecurityDebug = "security-debug"
)

// GetEnvPaths any command line data that comes after the files are .env file paths
// Any argument for application without '--' prefix is considered to be path to the
// environment file.
func GetEnvPaths() []string {
	args := os.Args[1:]
	if len(args) == 0 {
		return []string{}
	}

	paths := make([]string, 0)

	for _, arg := range args {
		if len(arg) < 4 {
			continue
		}

		lastPart := arg[len(arg)-4:]
		if lastPart != ".env" {
			continue
		}

		if arg[:2] == "--" {
			continue
		}
		paths = append(paths, arg)
	}

	return paths
}

// GetArguments Load arguments, not the environment variable paths.
// Arguments starts with '--'
func GetArguments(parent *log.Logger) []string {
	var logger *log.Logger
	if parent != nil {
		newLogger, err := parent.Child("argument")
		if err != nil {
			logger.Warn("parent.Child", "error", err)
			return []string{}
		}
		logger = &newLogger

		logger.Info("Supported app arguments",
			"--"+SECURE,
			"Enables security service",
			"--"+SecurityDebug,
			"To print the authentication logs",
		)
	}

	args := os.Args[1:]
	if len(args) == 0 {
		if logger != nil {
			logger.Info("No arguments were given")
		}
		return []string{}
	}

	parameters := make([]string, 0)

	for _, arg := range args {
		if arg[:2] == "--" {
			parameters = append(parameters, arg[2:])
		}
	}

	if logger != nil {
		logger.Info("All arguments read", "amount", len(parameters), "app parameters", parameters)
	}

	return parameters
}

// Exist This function is same as `env.HasArgument`,
// except `env.ArgumentExist()` loads arguments automatically.
func Exist(argument string) bool {
	return Has(GetArguments(nil), argument)
}

// ExtractValue Extracts the value of the argument if it has.
// The argument value comes after "=".
//
// This function gets the arguments from the CLI automatically.
//
// If the argument doesn't exist, then returns an empty string.
// Therefore, you should check for the argument existence by calling `argument.Exist()`
func ExtractValue(arguments []string, required string) (string, error) {
	found := ""
	for _, argument := range arguments {
		// doesn't have a value
		if argument == required {
			continue
		}

		length := len(required)
		if len(argument) > length && argument[:length] == required {
			found = argument
			break
		}
	}

	value, err := GetValue(found)
	if err != nil {
		return "", fmt.Errorf("GetValue for %s argument: %w", required, err)
	}

	return value, nil
}

// Value Extracts the value of the argument if it's exists.
// Similar to GetValue() but doesn't accept the
func Value(name string) (string, error) {
	return ExtractValue(GetArguments(nil), name)
}

// GetValue Extracts the value of the argument.
// Argument comes after '='
func GetValue(argument string) (string, error) {
	parts := strings.Split(argument, "=")
	if len(parts) != 2 {
		return "", fmt.Errorf("strings.split(`%s`) should has two parts", argument)
	}

	return parts[1], nil
}

// Has checks is the required argument exists among arguments or not.
func Has(arguments []string, required string) bool {
	for _, argument := range arguments {
		if argument == required {
			return true
		}

		length := len(required)
		if len(argument) > length && argument[:length] == required {
			return true
		}
	}

	return false
}