package route

import (
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"testing"

	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestHandleFuncSuite struct {
	suite.Suite

	handleX interface{}
	handle0 interface{}
	handle1 interface{}
	handle2 interface{}
	handle3 interface{}
	handleN interface{}
}

// Make sure that Account is set to five
// before each test
func (test *TestHandleFuncSuite) SetupTest() {
	// the second argument is invalid
	handleX := func(request message.Request, param string) message.Reply {
		return request.Ok(key_value.Empty())
	}
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

	test.handleX = handleX
	test.handle0 = handle0
	test.handle1 = handle1
	test.handle2 = handle2
	test.handle3 = handle3
	test.handleN = handleN
}

func (test *TestHandleFuncSuite) Test_0_DepAmount() {
	s := &test.Suite

	s.Require().Equal(-1, DepAmount(test.handleX))
	s.Require().Equal(0, DepAmount(test.handle0))
	s.Require().Equal(1, DepAmount(test.handle1))
	s.Require().Equal(2, DepAmount(test.handle2))
	s.Require().Equal(3, DepAmount(test.handle3))
	s.Require().Equal(4, DepAmount(test.handleN))
}

func (test *TestHandleFuncSuite) Test_1_Handle() {
	req := &message.Request{
		Command:    "ping",
		Parameters: key_value.Empty(),
	}

	deps4 := []*client.ClientSocket{nil, nil, nil, nil}
	deps3 := []*client.ClientSocket{nil, nil, nil}
	deps2 := []*client.ClientSocket{nil, nil}
	deps1 := []*client.ClientSocket{nil}
	var deps0 []*client.ClientSocket

	// Trying to pass invalid HandleFunc should fail
	reply := Handle(req, test.handleX, deps4)
	test.Suite.Require().False(reply.IsOK())

	// Trying to pass fewer deps to the HandleFuncN should fail
	reply = Handle(req, test.handleN, deps3)
	test.Suite.Require().False(reply.IsOK())

	// More than 3 deps passed, it should be successful
	reply = Handle(req, test.handleN, deps4)
	test.Suite.Require().True(reply.IsOK())

	// Trying to pass more deps to the HandleFunc<number> should fail
	reply = Handle(req, test.handle3, deps4)
	test.Suite.Require().False(reply.IsOK())

	// Trying to pass fewer deps to the HandleFunc<number> should fail
	reply = Handle(req, test.handle3, deps2)
	test.Suite.Require().False(reply.IsOK())

	// Passing the exact number of deps to HandleFunc<number> should be successful
	reply = Handle(req, test.handle3, deps3)
	test.Suite.Require().True(reply.IsOK())

	reply = Handle(req, test.handle2, deps2)
	test.Suite.Require().True(reply.IsOK())

	reply = Handle(req, test.handle1, deps1)
	test.Suite.Require().True(reply.IsOK())

	reply = Handle(req, test.handle0, deps0)
	test.Suite.Require().True(reply.IsOK())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHandleFunc(t *testing.T) {
	suite.Run(t, new(TestHandleFuncSuite))
}
