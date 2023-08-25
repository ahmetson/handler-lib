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
		// Used for testing 'message_amount' command.
		// While handling, the queue length should decrease.
		// While handling, the processing length should increase.
		time.Sleep(time.Second)
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

// Test_14_InstanceAmount trying check that instance amount is correct
func (test *TestHandlerManagerSuite) Test_14_InstanceAmount() {
	s := &test.Suite
	req := message.Request{Command: "instance_amount", Parameters: key_value.Empty()}

	// No instances were added, so it must return 0
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	instanceAmount, err := reply.Parameters.GetUint64("instance_amount")
	s.Require().NoError(err)
	s.Require().Zero(instanceAmount)

	// Add a new instance
	empty := key_value.Empty()
	instanceId, err := test.instanceManager.AddInstance(test.inprocConfig.Type, &test.routes, &empty, &empty)
	s.Require().NoError(err)

	// Wait a bit for instance initialization
	time.Sleep(time.Millisecond * 100)

	// The instance amount is not 0
	reply = test.req(req)
	s.Require().True(reply.IsOK())
	instanceAmount, err = reply.Parameters.GetUint64("instance_amount")
	s.Require().NoError(err)
	s.Require().NotZero(instanceAmount)

	//
	// After instance deletion, the instance_amount should return a correct result
	//
	err = test.instanceManager.DeleteInstance(instanceId)
	s.Require().NoError(err)

	// Wait a bit for the closing of the instance thread
	time.Sleep(time.Millisecond * 100)

	// Must be 0 instances
	reply = test.req(req)
	s.Require().True(reply.IsOK())
	instanceAmount, err = reply.Parameters.GetUint64("instance_amount")
	s.Require().NoError(err)
	s.Require().Zero(instanceAmount)

	test.cleanOut()
}

// Test_15_InstanceAmount checks that instance amount is correct when instances come and go
func (test *TestHandlerManagerSuite) Test_15_InstanceAmount() {
	s := &test.Suite
	req := message.Request{Command: "instance_amount", Parameters: key_value.Empty()}

	// No instances were added, so it must return 0
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	instanceAmount, err := reply.Parameters.GetUint64("instance_amount")
	s.Require().NoError(err)
	s.Require().Zero(instanceAmount)

	// Add a new instance
	empty := key_value.Empty()
	instanceId, err := test.instanceManager.AddInstance(test.inprocConfig.Type, &test.routes, &empty, &empty)
	s.Require().NoError(err)

	// Wait a bit for instance initialization
	time.Sleep(time.Millisecond * 100)

	// The instance amount is not 0
	reply = test.req(req)
	s.Require().True(reply.IsOK())
	instanceAmount, err = reply.Parameters.GetUint64("instance_amount")
	s.Require().NoError(err)
	s.Require().NotZero(instanceAmount)

	//
	// After instance deletion, the instance_amount should return a correct result
	//
	err = test.instanceManager.DeleteInstance(instanceId)
	s.Require().NoError(err)

	// Wait a bit for the closing of the instance thread
	time.Sleep(time.Millisecond * 100)

	// Must be 0 instances
	reply = test.req(req)
	s.Require().True(reply.IsOK())
	instanceAmount, err = reply.Parameters.GetUint64("instance_amount")
	s.Require().NoError(err)
	s.Require().Zero(instanceAmount)

	test.cleanOut()
}

// Test_16_MessageAmount checks that queue and processing messages amount are correct
func (test *TestHandlerManagerSuite) Test_16_MessageAmount() {
	s := &test.Suite
	req := message.Request{Command: "message_amount", Parameters: key_value.Empty()}

	// Imitating the user that sends the message
	clientType := config.ClientSocketType(test.inprocConfig.Type)
	clientSocket, err := zmq.NewSocket(clientType)
	s.Require().NoError(err)
	clientUrl := config.ExternalUrl(test.inprocConfig.Id, test.inprocConfig.Port)
	err = clientSocket.Connect(clientUrl)
	s.Require().NoError(err)

	// No instances were added, so it must return 0
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	queueAmount, err := reply.Parameters.GetUint64("queue_length")
	s.Require().NoError(err)
	s.Require().Zero(queueAmount)
	procAmount, err := reply.Parameters.GetUint64("processing_length")
	s.Require().NoError(err)
	s.Require().Zero(procAmount)

	// User sends a message
	extReq := message.Request{Command: "command_1", Parameters: key_value.Empty()}
	extReqStr, err := extReq.String()
	s.Require().NoError(err)
	_, err = clientSocket.SendMessageDontwait(extReqStr)
	s.Require().NoError(err)

	// Wait a bit for transfer between threads
	time.Sleep(time.Millisecond * 100)

	// Queue has one message
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	queueAmount, err = reply.Parameters.GetUint64("queue_length")
	s.Require().NoError(err)
	s.Require().NotZero(queueAmount)
	procAmount, err = reply.Parameters.GetUint64("processing_length")
	s.Require().NoError(err)
	s.Require().Zero(procAmount)

	// Add a new instance that will start processing the message
	empty := key_value.Empty()
	_, err = test.instanceManager.AddInstance(test.inprocConfig.Type, &test.routes, &empty, &empty)
	s.Require().NoError(err)

	// Wait a bit for instance initialization
	time.Sleep(time.Millisecond * 100)

	// The instance handles the request, so queue must be empty.
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	queueAmount, err = reply.Parameters.GetUint64("queue_length")
	s.Require().NoError(err)
	s.Require().Zero(queueAmount)
	procAmount, err = reply.Parameters.GetUint64("processing_length")
	s.Require().NoError(err)
	s.Require().NotZero(procAmount)

	// After handling, the queue and processing must be empty
	_, err = clientSocket.RecvMessage(0) // handling finished

	reply = test.req(req)
	s.Require().True(reply.IsOK())

	queueAmount, err = reply.Parameters.GetUint64("queue_length")
	s.Require().NoError(err)
	s.Require().Zero(queueAmount)
	procAmount, err = reply.Parameters.GetUint64("processing_length")
	s.Require().NoError(err)
	s.Require().Zero(procAmount)

	// clean out
	err = clientSocket.Close()
	s.Require().NoError(err)

	test.cleanOut()
}

// Test_17_MessageAmount checks that queue and processing messages amount are correct
func (test *TestHandlerManagerSuite) Test_17_MessageAmount() {
	s := &test.Suite
	req := message.Request{Command: "status", Parameters: key_value.Empty()}

	// Test setup runs all parts, status must be Ready
	reply := test.req(req)
	s.Require().True(reply.IsOK())

	status, err := reply.Parameters.GetString("status")
	s.Require().NoError(err)
	s.Require().Equal(Ready, status)

	//
	// Turn the status to incomplete
	//
	partReq := message.Request{Command: "close_part", Parameters: key_value.Empty().Set("part", "reactor")}
	reply = test.req(partReq)
	s.Require().True(reply.IsOK())

	// Wait a bit for the reactor closes itself
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(reactor.CREATED, test.reactor.Status())

	// Status must be incomplete
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.Parameters.GetString("status")
	s.Require().NoError(err)
	s.Require().Equal(Incomplete, status)

	// Only reactor must be incomplete
	parts, err := reply.Parameters.GetKeyValue("parts")
	s.Require().NoError(err)
	reactorStatus, err := parts.GetString("reactor")
	s.Require().NoError(err)
	s.Require().Equal(reactor.CREATED, reactorStatus)
	instanceManager, err := parts.GetString("instance_manager")
	s.Require().NoError(err)
	s.Require().Equal(instance_manager.Running, instanceManager)

	//
	// Absolutely incomplete if instance manager stopped
	//
	partReq.Parameters.Set("part", "instance_manager")
	reply = test.req(partReq)
	s.Require().True(reply.IsOK())

	// Wait a bit for the reactor closes itself
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(instance_manager.Idle, test.instanceManager.Status())

	// Status must be incomplete
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.Parameters.GetString("status")
	s.Require().NoError(err)
	s.Require().Equal(Incomplete, status)

	// Reactor and instance manager are incomplete
	parts, err = reply.Parameters.GetKeyValue("parts")
	s.Require().NoError(err)
	reactorStatus, err = parts.GetString("reactor")
	s.Require().NoError(err)
	s.Require().Equal(reactor.CREATED, reactorStatus)
	instanceManager, err = parts.GetString("instance_manager")
	s.Require().NoError(err)
	s.Require().Equal(instance_manager.Idle, instanceManager)

	//
	// Incomplete turns to ready when processes are running
	//

	// Run the instance manager
	partReq.Command = "run_part"
	reply = test.req(partReq)
	s.Require().True(reply.IsOK())

	// Wait a bit for instance manager initialization
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(instance_manager.Running, test.instanceManager.Status())

	// Status must be incomplete
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.Parameters.GetString("status")
	s.Require().NoError(err)
	s.Require().Equal(Incomplete, status)

	// Only reactor is incomplete
	parts, err = reply.Parameters.GetKeyValue("parts")
	s.Require().NoError(err)
	reactorStatus, err = parts.GetString("reactor")
	s.Require().NoError(err)
	s.Require().Equal(reactor.CREATED, reactorStatus)
	instanceManager, err = parts.GetString("instance_manager")
	s.Require().NoError(err)
	s.Require().Equal(instance_manager.Running, instanceManager)

	// Run Reactor
	partReq.Parameters.Set("part", "reactor")
	reply = test.req(partReq)
	s.Require().True(reply.IsOK())

	// Wait a bit for reactor initialization
	time.Sleep(time.Millisecond * 100)
	s.Require().Equal(reactor.RUNNING, test.reactor.Status())

	// Status must be ready
	reply = test.req(req)
	s.Require().True(reply.IsOK())

	status, err = reply.Parameters.GetString("status")
	s.Require().NoError(err)
	s.Require().Equal(Ready, status)

	// Clean
	test.cleanOut()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHandlerManager(t *testing.T) {
	suite.Run(t, new(TestHandlerManagerSuite))
}
