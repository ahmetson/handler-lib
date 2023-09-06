package config

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestConfigSuite struct {
	suite.Suite
}

func (test *TestConfigSuite) SetupTest() {
}

// Test_11_Deps tests setting of the route dependencies
func (test *TestConfigSuite) Test_10_IsValid() {
	s := test.Suite.Require

	s().Error(IsValid(UnknownType))
	s().NoError(IsValid(PublisherType))
}

// a normal test function and pass our suite to suite.Run
func TestConfig(t *testing.T) {
	suite.Run(t, new(TestConfigSuite))
}
