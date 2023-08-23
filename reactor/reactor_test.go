package reactor

import (
	"fmt"
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
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

// Test_10_External tests the external socket is running and the messages are queued
func (test *TestReactorSuite) Test_10_External() {
	s := &test.Suite

	// The reactor has no info about the handler
	s.Require().Nil(test.reactor.externalConfig)

	test.reactor.SetConfig(test.handleConfig)

	// Queue is populated by External socket, so let's test it.
	test.reactor.queueCap = 2
	err := test.reactor.queue.SetCap(2)
	s.Require().NoError(err)

	// The reactor has the configuration of the handler
	s.Require().NotNil(test.reactor.externalConfig)

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
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestReactor(t *testing.T) {
	suite.Run(t, new(TestReactorSuite))
}
