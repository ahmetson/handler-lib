package reactor

import (
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/log-lib"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestReactorSuite struct {
	suite.Suite

	reactor      *Reactor
	handleConfig *config.Handler
}

func (test *TestReactorSuite) SetupTest() {
	test.handleConfig = &config.Handler{Type: config.SyncReplierType, Category: "test", Id: "sample", Port: 0, InstanceAmount: 1}
}

func (test *TestReactorSuite) Test_0_New() {
	test.reactor = New()
}

func (test *TestReactorSuite) Test_10_SetConfig() {
	s := &test.Suite

	// The reactor has no info about the handler
	s.Require().Nil(test.reactor.externalConfig)

	test.reactor.SetConfig(test.handleConfig)

	// The reactor has the configuration of the handler
	s.Require().NotNil(test.reactor.externalConfig)
}

// Test_11_External tests the external socket is running and the messages are queued.
//
// It doesn't send the messages to the consumer.
func (test *TestReactorSuite) Test_11_External() {
	s := &test.Suite

	// Queue is populated by External socket, so let's test it.
	err := test.reactor.queue.SetCap(2)
	s.Require().NoError(err)

	// Let's prepare the external socket from the handle configuration
	err = test.reactor.prepareExternalSocket()
	s.Require().NoError(err)

	// user that will send the messages
	clientUrl := client.ClientUrl(test.reactor.externalConfig.Id, test.reactor.externalConfig.Port)
	user, err := zmq.NewSocket(zmq.DEALER)
	s.Require().NoError(err)
	err = user.Connect(clientUrl)
	s.Require().NoError(err)

	// before external socket receives the messages, we must be sure queue is valid
	s.Require().True(test.reactor.queue.IsEmpty())
	s.Require().EqualValues(test.reactor.queue.Len(), uint(0))

	for i := 1; i <= 2; i++ {
		msg := message.Request{Command: "cmd", Parameters: key_value.Empty().Set("id", i).Set("cap", 2)}
		msgStr, err := msg.String()
		s.Require().NoError(err)
		_, err = user.SendMessage("", msgStr)
		s.Require().NoError(err)
	}

	// A delay a bit, until frontend will catch the user messages
	time.Sleep(time.Millisecond * 50)
	err = test.reactor.handleFrontend()
	s.Require().NoError(err)
	err = test.reactor.handleFrontend()
	s.Require().NoError(err)

	s.Require().False(test.reactor.queue.IsEmpty())
	s.Require().True(test.reactor.queue.IsFull())
	s.Require().EqualValues(test.reactor.queue.Len(), uint(2))

	// If we try to send a message, it should fail
	i := 3
	msg := message.Request{Command: "cmd", Parameters: key_value.Empty().Set("id", i).Set("cap", 2)}
	msgStr, err := msg.String()
	s.Require().NoError(err)
	_, err = user.SendMessage("", msgStr)
	s.Require().NoError(err)

	err = test.reactor.handleFrontend()
	s.Require().NoError(err)

	// However, the third message that user receives should be a failure
	reply, err := user.RecvMessage(0)
	s.Require().NoError(err)
	rep, err := message.ParseReply(reply)
	s.Require().NoError(err)
	s.Require().False(rep.IsOK())

	// Trying to decrease the length should fail
	err = test.reactor.queue.SetCap(1)
	s.Require().Error(err)

	// Clean out
	err = user.Close()
	s.Require().NoError(err)

	err = test.reactor.external.Close()
	s.Require().NoError(err)

	// clean out the queue
	test.reactor.queue = data_type.NewQueue()
}

// Test_12_Consumer tests the consuming
func (test *TestReactorSuite) Test_12_Consumer() {
	s := &test.Suite

	// Consumer requires Instance Manager
	err := test.reactor.handleConsume()
	s.Require().Error(err)

	// Added Instance Manager
	cmd, instanceId, instanceManager := test.instanceManager()

	test.reactor.SetInstanceManager(instanceManager) // adding

	// Empty queue does nothing
	s.Require().True(test.reactor.queue.IsEmpty())

	// Instance manager is set, zeromq reactor is set, but no queue should do nothing
	err = test.reactor.handleConsume()
	s.Require().Error(err)

	// Also, consumer requires zeromq reactor
	test.reactor.prepareSockets()

	// Instance manager is set, zeromq reactor is set, but no queue should do nothing
	err = test.reactor.handleConsume()
	s.Require().NoError(err)

	// add a message into the queue
	req := message.Request{Command: cmd, Parameters: key_value.Empty()}
	reqStr, err := req.String()
	s.Require().NoError(err)
	messageId := "msg_1"

	s.Require().True(test.reactor.queue.IsEmpty())
	test.reactor.queue.Push([]string{messageId, "", reqStr})
	s.Require().False(test.reactor.queue.IsEmpty())

	// Before testing handle consume with queue,
	// make sure that processing is empty.
	// Consuming the message should remove the message from queue and add it to the processing list
	s.Require().True(test.reactor.processing.IsEmpty())

	// Consuming the message
	err = test.reactor.handleConsume()
	s.Require().NoError(err)

	// The consumed message should be moved from queue to the processing
	s.Require().True(test.reactor.queue.IsEmpty())
	s.Require().False(test.reactor.processing.IsEmpty())

	// The processing list has the expected tracking. Such as instance handles the message id
	rawMessageId, err := test.reactor.processing.Get(instanceId)
	s.Require().NoError(err)
	s.Require().EqualValues(messageId, rawMessageId.(string))

	// clean out
	instanceManager.Close()
	test.reactor.processing = key_value.NewList()

	time.Sleep(time.Millisecond * 100) // wait until instance manager ends
}

// Create an instance manager. Returns a test command, instance id and instance manager itself
func (test *TestReactorSuite) instanceManager() (string, string, *instance_manager.Parent) {
	s := &test.Suite

	cmd := "hello"
	handleHello := func(req message.Request) message.Reply {
		time.Sleep(time.Second) // just to test consuming since there are no ready instances
		id, err := req.Parameters.GetUint64("id")
		if err != nil {
			id = 0
		}
		return req.Ok(key_value.Empty().Set("id", id))
	}
	routes := key_value.Empty().Set(cmd, handleHello)
	routeDeps := key_value.Empty()
	depClients := key_value.Empty()

	// Added Instance Manager
	logger, err := log.New(test.handleConfig.Id, true)
	s.Require().NoError(err)
	instanceManager := instance_manager.New(test.handleConfig.Id, logger)

	go instanceManager.Run()

	time.Sleep(time.Millisecond * 50) // wait until it updates the status
	instanceId, err := instanceManager.AddInstance(test.handleConfig.Type, &routes, &routeDeps, &depClients)
	s.Require().NoError(err)

	time.Sleep(time.Millisecond * 50) // wait until the instance will be loaded

	return cmd, instanceId, instanceManager
}

// Test_13_Run runs the Reactor
func (test *TestReactorSuite) Test_13_Run() {
	s := &test.Suite

	// Queue is populated by External socket, so let's test it.
	err := test.reactor.queue.SetCap(1)
	s.Require().NoError(err)

	// The reactor runs for the first time
	s.Require().Equal(CREATED, test.reactor.Status())

	cmd, _, instanceManager := test.instanceManager()
	test.reactor.SetInstanceManager(instanceManager)

	// Run
	go test.reactor.Run()

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 10)

	// Running?
	s.Require().Equal(RUNNING, test.reactor.Status())

	// The external user sends the request.
	// The request is handled by one instance.
	//
	// Assuming each request is handled in 1 second.
	// User sends the first request to make the instance busy.
	// He then sends the second request that should be queued
	// (before 1 second of instance handling expires).
	// User sends the third request
	// (before 1 second of instance handling expires).
	// The third request should not be added to the queue, since the queue is full.
	// User will be idle
	clientUrl := client.ClientUrl(test.reactor.externalConfig.Id, test.reactor.externalConfig.Port)
	user, err := zmq.NewSocket(zmq.DEALER)
	s.Require().NoError(err)
	err = user.Connect(clientUrl)
	s.Require().NoError(err)

	req := message.Request{Command: cmd, Parameters: key_value.Empty().Set("id", 1)}
	reqStr, err := req.String()
	s.Require().NoError(err)

	// Everything should be empty
	s.Require().True(test.reactor.queue.IsEmpty())
	s.Require().True(test.reactor.processing.IsEmpty())

	// The first message consumed instantly
	_, err = user.SendMessage("", reqStr)
	s.Require().NoError(err)

	// Delay a bit for socket transfers
	time.Sleep(time.Millisecond * 50)

	// It's processing?
	s.Require().True(!test.reactor.queue.IsEmpty() || !test.reactor.processing.IsEmpty())

	// Sending the second message
	s.Require().True(test.reactor.queue.IsEmpty())
	req.Parameters.Set("id", 2)
	reqStr, err = req.String()
	s.Require().NoError(err)

	_, err = user.SendMessage("", reqStr)
	s.Require().NoError(err)

	// Wait a bit for transmission between sockets
	time.Sleep(time.Millisecond * 50)

	// The reactor queued
	s.Require().True(test.reactor.queue.IsFull())

	// Sending the third message
	req.Parameters.Set("id", 3)
	reqStr, err = req.String()
	s.Require().NoError(err)

	_, err = user.SendMessage("", reqStr)
	s.Require().NoError(err)

	// Wait a bit for transmission between sockets
	time.Sleep(time.Millisecond * 50)

	// Now we receive the messages
	repl, err := user.RecvMessage(0)
	s.Require().NoError(err)
	errorRepl, err := message.ParseReply(repl)
	s.Require().NoError(err)
	s.Require().False(errorRepl.IsOK())
	_, err = user.RecvMessage(0)
	s.Require().NoError(err)
	_, err = user.RecvMessage(0)
	s.Require().NoError(err)

	// Close the reactor
	err = test.reactor.Close()
	s.Require().NoError(err)

	// wait a bit before the socket runner stops
	time.Sleep(time.Millisecond * 50)
	s.Require().Equal(CREATED, test.reactor.Status())

	err = user.Close()
	s.Require().NoError(err)

}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestReactor(t *testing.T) {
	suite.Run(t, new(TestReactorSuite))
}
