package instance_manager

import (
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/log-lib"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestInstanceSuite struct {
	suite.Suite

	parent   *Parent
	handle0  interface{}
	handle1  interface{}
	parentId string

	clients   key_value.KeyValue
	routes    key_value.KeyValue
	routeDeps key_value.KeyValue
}

// Make sure that Account is set to five
// before each test
func (test *TestInstanceSuite) SetupTest() {
	handle0 := func(request message.Request) message.Reply {
		time.Sleep(time.Millisecond * 200)
		return request.Ok(key_value.Empty())
	}
	handle1 := func(request message.Request, _ *client.ClientSocket) message.Reply {
		return request.Ok(key_value.Empty())
	}

	test.handle0 = handle0
	test.handle1 = handle1
	test.parentId = "parent_0"
}

func (test *TestInstanceSuite) Test_0_New() {
	s := &test.Suite

	logger, _ := log.New("parent_test", true)

	test.parent = New(test.parentId, logger)

	s.Require().Equal(Idle, test.parent.Status())
}

// Test_10_Close running the instance manager
func (test *TestInstanceSuite) Test_10_Close() {
	s := &test.Suite

	// First, it should be prepared
	s.Require().Equal(Idle, test.parent.Status())

	// Let's run the service
	go test.parent.Run()
	time.Sleep(time.Millisecond * 100) // waiting a time for initialization

	// Make sure that the service is running
	s.Require().Equal(Running, test.parent.Status())

	// Sending a close message
	test.parent.Close()

	// Waiting
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(Idle, test.parent.Status())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInstance(t *testing.T) {
	suite.Run(t, new(TestInstanceSuite))
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInstance(t *testing.T) {
	suite.Run(t, new(TestInstanceSuite))
}
