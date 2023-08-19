package instance

import (
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"testing"

	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestInstanceSuite struct {
	suite.Suite

	instance0 *Instance
	handle0   interface{}
	handle1   interface{}
	handle2   interface{}
	handle3   interface{}
	handleN   interface{}
}

// Make sure that Account is set to five
// before each test
func (test *TestInstanceSuite) SetupTest() {
	handle0 := func(request message.Request) message.Reply {
		return request.Ok(key_value.Empty())
	}
	handle1 := func(request message.Request, _ *client.ClientSocket) message.Reply {
		return request.Ok(key_value.Empty())
	}
	handle2 := func(request message.Request, _ *client.ClientSocket, _ *client.ClientSocket) message.Reply {
		return request.Ok(key_value.Empty())
	}
	handle3 := func(request message.Request, _ *client.ClientSocket, _ *client.ClientSocket, _ *client.ClientSocket) message.Reply {
		return request.Ok(key_value.Empty())
	}
	handleN := func(request message.Request, _ ...*client.ClientSocket) message.Reply {
		return request.Ok(key_value.Empty())
	}

	test.handle0 = handle0
	test.handle1 = handle1
	test.handle2 = handle2
	test.handle3 = handle3
	test.handleN = handleN
}

func (test *TestInstanceSuite) Test_0_New() {
	handlerType := config.SyncReplierType
	id := "instance_0"
	parentId := "parent_0"

	test.instance0 = New(handlerType, id, parentId)
}

// Test_1_SetRoutes tests the setting routes references from instance.
// If the routes are changed by the parent, then instances should have the updated routes.
// Let's test it here. We imitate a parent. And set the routes.
// Then we update the route.
func (test *TestInstanceSuite) Test_1_SetRoutes() {
	s := &test.Suite

	routes := key_value.Empty()
	routeDeps := key_value.Empty()

	// Before setting the routes, the instance should have a nil there
	s.Require().Nil(test.instance0.routes)
	s.Require().Nil(test.instance0.routeDeps)

	// Update the routes
	test.instance0.SetRoutes(&routes, &routeDeps)

	// Now, the instance should have the empty routes since we added empty routes
	s.Require().NotNil(test.instance0.routes)
	s.Require().NotNil(test.instance0.routeDeps)
	s.Require().Len(*test.instance0.routes, 0)
	s.Require().Len(*test.instance0.routeDeps, 0)

	// Let's imitate the handler updated the routes
	routes.Set("handle_0", test.handle0)
	s.Require().Len(*test.instance0.routes, 1)
	s.Require().Len(*test.instance0.routeDeps, 0)

	// Let's imitate that handler updated the route dependencies
	routes.Set("handle_1", test.handle1)
	routeDeps.Set("handle_1", []string{"dep_1"})
	s.Require().Len(*test.instance0.routes, 2)
	s.Require().Len(*test.instance0.routeDeps, 1)

	// Make sure that instance's routes lint to the valid parameters.
	index := 0
	for cmdName := range *test.instance0.routes {
		routeIndex := 0
		for routeCmdName := range routes {
			if index == routeIndex {
				s.Require().Equal(routeCmdName, cmdName)
				break
			}

			routeIndex++
		}

		index++
	}

	// Make sure that route deps lint to the valid parameters.
	index = 0
	for cmdName := range *test.instance0.routeDeps {
		routeIndex := 0
		for routeCmdName := range routeDeps {
			if index == routeIndex {
				s.Require().Equal(routeCmdName, cmdName)
				break
			}

			routeIndex++
		}

		index++
	}

}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestInstance(t *testing.T) {
	suite.Run(t, new(TestInstanceSuite))
}
