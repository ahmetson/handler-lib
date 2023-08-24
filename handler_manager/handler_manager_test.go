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
	routes       map[string]interface{}
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
	test.routes = make(map[string]interface{}, 2)
	test.routes["command_1"] = func(request message.Request) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	}
	test.routes["command_2"] = func(request message.Request) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	}

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

	// must fail since command is invalid
	req := message.Request{Command: "no_command", Parameters: key_value.Empty()}
	reply := test.req(req)
	s.Require().False(reply.IsOK())

	test.cleanOut()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHandlerManager(t *testing.T) {
	suite.Run(t, new(TestHandlerManagerSuite))
}
