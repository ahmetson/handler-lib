package base

import (
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/log-lib"
	"github.com/stretchr/testify/suite"
	"testing"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestHandlerSuite struct {
	suite.Suite
	tcpHandler    *Handler
	inprocHandler *Handler
	tcpConfig     *config.Handler
	inprocConfig  *config.Handler
	tcpClient     *client.ClientSocket
	inprocClient  *client.ClientSocket
	logger        *log.Logger
	routes        map[string]interface{}
}

// Todo test in-process and external types of controllers
// Todo test the business of the server
// Make sure that Account is set to five
// before each test
func (test *TestHandlerSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("handler", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.tcpHandler = New()
	test.inprocHandler = New()

	// Socket to talk to clients
	test.routes = make(map[string]interface{}, 2)
	test.routes["command_1"] = func(request message.Request) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	}
	test.routes["command_2"] = func(request message.Request) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	}

	err = test.inprocHandler.Route("command_1", test.routes["command_1"])
	s.Require().NoError(err)
	err = test.inprocHandler.Route("command_2", test.routes["command_2"])
	s.Require().NoError(err)

	test.inprocConfig = config.NewInternalHandler(config.SyncReplierType, "test")
	test.tcpConfig, err = config.NewHandler(config.SyncReplierType, "test")
	s.Require().NoError(err)

	//go func() {
	//	_ = test.inprocHandler.Run()
	//}()
	//go func() {
	//	_ = test.tcpHandler.Run()
	//}()

	// Run for the controllers to be ready
	//time.Sleep (time.Millisecond * 100)
}

// Test_10_Sets tests setting of the configuration and logger
func (test *TestHandlerSuite) Test_10_Sets() {
	s := &test.Suite

	// Setting a logger should fail since we don't have a configuration set
	s.Require().Error(test.inprocHandler.SetLogger(test.logger))

	// Setting the configuration
	test.inprocHandler.SetConfig(test.inprocConfig)

	// Setting the logger should be successful
	s.Require().NoError(test.inprocHandler.SetLogger(test.logger))

	// Setting the parameters of the Tcp Handler
	test.tcpHandler.SetConfig(test.tcpConfig)
	s.Require().NoError(test.tcpHandler.SetLogger(test.logger))
}

// Test_11_Deps tests setting of the route dependencies
func (test *TestHandlerSuite) Test_11_Deps() {
	s := &test.Suite

	// Handler must not have dependencies yet
	s.Require().Empty(test.inprocHandler.DepIds())
	s.Require().Empty(test.tcpHandler.DepIds())

	test.routes["command_3"] = func(request message.Request, _ *client.ClientSocket, _ *client.ClientSocket) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	}

	// Adding a new route with the dependencies
	err := test.inprocHandler.Route("command_3", test.routes["command_3"], "dep_1", "dep_2")
	s.Require().NoError(err)
	err = test.tcpHandler.Route("command_3", test.routes["command_3"], "dep_1", "dep_2")
	s.Require().NoError(err)

	s.Require().Len(test.inprocHandler.DepIds(), 2)
	s.Require().Len(test.tcpHandler.DepIds(), 2)

	// Trying to route the handler with inconsistent dependencies must fail
	err = test.tcpHandler.Route("command_4", test.routes["command_3"]) // command_3 handler requires two dependencies
	s.Require().Error(err)

	err = test.tcpHandler.Route("command_5", test.routes["command_2"], "dep_1", "dep_2") // command_2 handler not requires any dependencies
	s.Require().Error(err)

	// Adding a new command with already added dependency should be fine
	test.routes["command_4"] = func(request message.Request, _ *client.ClientSocket, _ *client.ClientSocket) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	}
	err = test.inprocHandler.Route("command_4", test.routes["command_4"], "dep_1", "dep_3") // command_3 handler requires two dependencies
	s.Require().NoError(err)

	depIds := test.inprocHandler.DepIds()
	s.Require().Len(depIds, 3)
	s.Require().EqualValues([]string{"dep_1", "dep_2", "dep_3"}, depIds)
}
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (test *TestHandlerSuite) TestRun() {
	//var wg sync.WaitGroup

	//wg.Add(1)
	//// tcp client
	//go func() {
	//	// no route found
	//	request3 := message.Request{
	//		Command:    "command_3",
	//		Parameters: key_value.Empty(),
	//	}
	//	_, err := test.tcpClient.RequestRemoteService(&request3)
	//	test.Require().Error(err)
	//
	//	wg.Done()
	//}()

	//wg.Wait()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestReplyController(t *testing.T) {
	suite.Run(t, new(TestHandlerSuite))
}
