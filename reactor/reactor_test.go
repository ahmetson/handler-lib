package reactor

import (
	"fmt"
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

// Test_10_External tests the external socket is running and the messages are queued
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
		fmt.Printf("user sending %d/%d\n", i, 2)
		msg := message.Request{Command: "cmd", Parameters: key_value.Empty().Set("id", i).Set("cap", 2)}
		msgStr, err := msg.String()
		s.Require().NoError(err)
		_, err = user.SendMessage("", msgStr)
		s.Require().NoError(err)
		fmt.Printf("user sent %d/%d\n", i, 2)
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
	fmt.Printf("user sending %d/%d\n", i, 2)
	msg := message.Request{Command: "cmd", Parameters: key_value.Empty().Set("id", i).Set("cap", 2)}
	msgStr, err := msg.String()
	s.Require().NoError(err)
	_, err = user.SendMessage("", msgStr)
	s.Require().NoError(err)
	fmt.Printf("user sent %d/%d\n", i, 2)

	fmt.Printf("external waits %d/%d\n", i, 2)
	err = test.reactor.handleFrontend()
	s.Require().Error(err)
	fmt.Printf("external received: %d/%d with error: %v\n", i, 2, err)

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

// Test_11_Consumer tests the consuming
func (test *TestReactorSuite) Test_12_Consumer() {
	s := &test.Suite
	cmd := "hello"
	handleHello := func(req message.Request) message.Reply {
		time.Sleep(time.Second) // just to test consuming since there is no ready instances
		return req.Ok(key_value.Empty())
	}
	routes := key_value.Empty().Set(cmd, handleHello)
	routeDeps := key_value.Empty()
	depClients := key_value.Empty()

	// Consumer requires Instance Manager
	err := test.reactor.handleConsume()
	s.Require().Error(err)

	// Added Instance Manager
	logger, err := log.New(test.handleConfig.Id, true)
	s.Require().NoError(err)
	instanceManager := instance_manager.New(test.handleConfig.Id, logger)

	go instanceManager.Run()

	time.Sleep(time.Millisecond * 50) // wait until it updates the status
	instanceId, err := instanceManager.AddInstance(test.handleConfig.Type, &routes, &routeDeps, &depClients)
	s.Require().NoError(err)

	time.Sleep(time.Millisecond * 50) // wait until the instance will be loaded

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
	// Consuming the message should remove the message from queue, and add it to the processing list
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

	time.Sleep(time.Millisecond * 100) // wait until instance manager ends
}

// Test_12_Run runs the Reactor
func (test *TestReactorSuite) Test_13_Run() {
	s := &test.Suite

	// Queue is populated by External socket, so let's test it.
	err := test.reactor.queue.SetCap(1)
	s.Require().NoError(err)

	// The reactor runs for the first time
	s.Require().Equal(CREATED, test.reactor.Status())

	// Run
	go test.reactor.Run()

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 10)

	// Running?
	s.Require().Equal(RUNNING, test.reactor.Status())

	// Close the reactor
	err = test.reactor.Close()
	s.Require().NoError(err)

	// wait a bit before the socket runner stops
	time.Sleep(time.Millisecond * 50)
	s.Require().Equal(CREATED, test.reactor.Status())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestReactor(t *testing.T) {
	suite.Run(t, new(TestReactorSuite))
}
