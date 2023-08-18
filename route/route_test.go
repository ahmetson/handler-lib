package route

import (
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"testing"

	"github.com/ahmetson/client-lib"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestRouteSuite struct {
	suite.Suite
	controller *zmq.Socket
	client     *client.ClientSocket
}

// Make sure that Account is set to five
// before each test
func (test *TestRouteSuite) SetupTest() {}

func (test *TestRouteSuite) Test_0_FilterClients() {
	s := &test.Suite
	deps := []string{"dep_1", "dep_2", "dep_3"}

	// Always returns the same length as dep amount
	filtered := FilterExtensionClients(deps, nil)
	s.Require().Len(filtered, 3)
	// However, they will be nils
	s.Require().Nil(filtered[0])
	s.Require().Nil(filtered[1])
	s.Require().Nil(filtered[2])

	// Returning the clients, the missing clients should be marked as nil
	dep1 := &client.ClientSocket{}
	dep2 := &client.ClientSocket{}
	dep3 := &client.ClientSocket{}
	dep4 := &client.ClientSocket{}
	clients := key_value.Empty().Set("dep_1", dep1).Set("dep_3", dep3)

	filtered = FilterExtensionClients(deps, clients)
	s.Require().Len(filtered, 3)
	s.Require().NotNil(filtered[0])
	s.Require().Nil(filtered[1])
	s.Require().NotNil(filtered[2])

	// There should not be any nils if the clients exist in the client list
	clients.Set("dep_2", dep2).Set("dep_4", dep4)
	filtered = FilterExtensionClients(deps, clients)
	s.Require().Len(filtered, 3)
	s.Require().NotNil(filtered[0])
	s.Require().NotNil(filtered[1])
	s.Require().NotNil(filtered[2])
}

func (test *TestRouteSuite) Test_1_Route() {
	s := &test.Suite
	handlers := key_value.Empty()
	deps := key_value.Empty()
	var anyHandle = func(request message.Request, _ *client.ClientSocket) message.Reply {
		return request.Ok(key_value.Empty())
	}
	var emptyHandle = func(request message.Request) message.Reply {
		return request.Ok(key_value.Empty())
	}
	anyDeps := []string{"dep_1"}
	cmd := "cmd"

	// Trying to route unregistered command should fail
	_, _, err := Route(cmd, handlers, deps)
	s.Require().Error(err)

	// Trying to route unregistered command, when any command is supported should return any
	handlers.Set(Any, anyHandle)
	deps.Set(Any, anyDeps)

	handleInterface, handleDeps, err := Route(cmd, handlers, deps)
	s.Require().NoError(err)
	_, ok := handleInterface.(HandleFunc1)
	s.Require().True(ok)
	s.Require().Len(handleDeps, len(anyDeps))
	s.Require().EqualValues(handleDeps, anyDeps)

	// Routing to the existing function should be successful
	handlers.Set(cmd, emptyHandle)
	handleInterface, handleDeps, err = Route(cmd, handlers, deps)
	s.Require().NoError(err)
	_, ok = handleInterface.(HandleFunc0)
	s.Require().True(ok)
	s.Require().Empty(handleDeps)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRoute(t *testing.T) {
	suite.Run(t, new(TestRouteSuite))
}
