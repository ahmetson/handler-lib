package handler_manager

import (
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/handler-lib/reactor"
	"github.com/ahmetson/log-lib"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestHandlerManagerSuite struct {
	suite.Suite

	instanceManager *instance_manager.Parent
	instanceRunner  func()
	reactor         *reactor.Reactor

	handlerManager *HandlerManager

	inprocConfig *config.Handler
	inprocClient *zmq.Socket
	logger       *log.Logger
	routes       key_value.KeyValue
}

// Make sure that Account is set to five
// before each test unit
func (test *TestHandlerManagerSuite) SetupTest() {
	s := &test.Suite

	test.inprocConfig = config.NewInternalHandler(config.SyncReplierType, "test")

	logger, err := log.New("handler", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.instanceManager = instance_manager.New(test.inprocConfig.Id, test.logger)

	test.instanceRunner = func() {
		go test.instanceManager.Run()
	}

	// Socket to talk to clients
	test.routes = key_value.Empty()
	test.routes.Set("command_1", func(request message.Request) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	})
	test.routes.Set("command_2", func(request message.Request) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	})

	test.reactor = reactor.New()
	test.reactor.SetConfig(test.inprocConfig)
	test.reactor.SetInstanceManager(test.instanceManager)

	test.handlerManager = New(test.reactor, test.instanceManager, test.instanceRunner)
	test.handlerManager.SetConfig(test.inprocConfig)

	go test.instanceManager.Run()
	go test.reactor.Run()
	go func() {
		err = test.handlerManager.Run()
		s.Require().NoError(err)
	}()

	// Wait a bit before parts are initialized
	time.Sleep(time.Millisecond * 100)

	// make sure that parts are running
	s.Require().Equal(instance_manager.Running, test.instanceManager.Status())
	s.Require().Equal(reactor.RUNNING, test.reactor.Status())
	s.Require().Equal(SocketReady, test.handlerManager.status)

	// Client that will imitate the service
	inprocClient, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	err = inprocClient.Connect(config.ManagerUrl(test.inprocConfig.Id))
	s.Require().NoError(err)
	test.inprocClient = inprocClient
}

// cleanOut everything
func (test *TestHandlerManagerSuite) cleanOut() {
	s := &test.Suite

	err := test.inprocClient.Close()
	s.Require().NoError(err)

	if test.instanceManager.Status() == instance_manager.Running {
		test.instanceManager.Close()
	}

	if test.reactor.Status() == reactor.RUNNING {
		err = test.reactor.Close()
		s.Require().NoError(err)
	}

	if test.handlerManager.status == SocketReady {
		test.handlerManager.Close()
	}

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything is closed
	s.Require().Equal(instance_manager.Idle, test.instanceManager.Status())
	s.Require().Equal(reactor.CREATED, test.reactor.Status())
	s.Require().Equal(SocketIdle, test.handlerManager.status)
}

func (test *TestHandlerManagerSuite) req(request message.Request) message.Reply {
	s := &test.Suite

	reqStr, err := request.String()
	s.Require().NoError(err)

	_, err = test.inprocClient.SendMessage(reqStr)
	s.Require().NoError(err)

	raw, err := test.inprocClient.RecvMessage(0)
	s.Require().NoError(err)

	reply, err := message.ParseReply(raw)
	s.Require().NoError(err)

	return reply
}

// Test_10_InvalidCommand tries to send an invalid command
func (test *TestHandlerManagerSuite) Test_10_InvalidCommand() {
	s := &test.Suite

	// must fail since the command is invalid
	req := message.Request{Command: "no_command", Parameters: key_value.Empty()}
	reply := test.req(req)
	s.Require().False(reply.IsOK())

	test.cleanOut()
}

// Test_12_ClosePart stops the parts
func (test *TestHandlerManagerSuite) Test_12_ClosePart() {
	s := &test.Suite
	params := key_value.Empty()
	req := message.Request{Command: "close_part", Parameters: params}

	// Trying to stop without a part must fail
	reply := test.req(req)
	s.Require().False(reply.IsOK())

	// Trying to stop a part that doesn't exist must fail
	params.Set("part", "no_part")
	req.Parameters = params
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	// Stopping the reactor must succeed
	params.Set("part", "reactor")
	req.Parameters = params
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Re-stopping the reactor must fail
	time.Sleep(time.Millisecond * 100)
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	// Stopping the instance manager must succeed
	params.Set("part", "instance_manager")
	req.Parameters = params
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Re-stopping the reactor must fail
	time.Sleep(time.Millisecond * 100)
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	test.cleanOut()
}

// Test_13_RunPart trying to run some parts
func (test *TestHandlerManagerSuite) Test_13_RunPart() {
	s := &test.Suite
	params := key_value.Empty()
	req := message.Request{Command: "close_part", Parameters: params}

	// Stopping the reactor that was run during test setup
	params.Set("part", "reactor")
	req.Parameters = params
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	// Make sure the reactor stopped
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(reactor.CREATED, test.reactor.Status())

	// Let's test running it
	req.Command = "run_part"
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Make sure it's running
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(reactor.RUNNING, test.reactor.Status())

	//
	// Testing with the instance manager
	//

	// stop the instance manager that was run during test setup
	req.Command = "close_part"
	params.Set("part", "instance_manager")
	req.Parameters = params

	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Make sure that instance manager stopped
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(instance_manager.Idle, test.instanceManager.Status())

	// Run the instance manager
	req.Command = "run_part"
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	// Make sure that instance manager is running
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(instance_manager.Running, test.instanceManager.Status())

	//
	// Re-running must fail
	//
	reply = test.req(req)
	s.Require().False(reply.IsOK())

	test.cleanOut()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHandlerManager(t *testing.T) {
	suite.Run(t, new(TestHandlerManagerSuite))
}
