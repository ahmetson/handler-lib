package sync_replier

import (
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/log-lib"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestHandlerSuite struct {
	suite.Suite
	pub           *Publisher
	config        *config.Trigger
	managerClient *zmq.Socket
	sub           *zmq.Socket
	trigger       *client.Socket
	logger        *log.Logger

	subscribed  chan []string
	closeClient bool
	poller      *zmq.Poller
}

// Make sure that Account is set to five
// before each test
func (test *TestHandlerSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("publisher", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.pub = New()

	handlerConfig := config.NewInternalHandler(config.SyncReplierType, "test")
	triggerConfig, err := config.InternalTriggerAble(handlerConfig, config.PublisherType)
	s.Require().NoError(err)
	test.config = triggerConfig

	// Setting a logger should fail since we don't have a configuration set
	s.Require().Error(test.pub.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.pub.SetConfig(test.config)
	s.Require().Equal(config.ReplierType, test.pub.base.Config.Type) // the trigger is rewritten
	s.Require().NoError(test.pub.SetLogger(test.logger))

	// running the trigger
	triggerClientConfig := test.pub.TriggerClient()
	triggerClient, err := client.New(triggerClientConfig)
	s.Require().NoError(err)
	test.trigger = triggerClient

	go test.subscribe()
	// wait a bit for initialization
	time.Sleep(time.Millisecond * 50)
}

// subscribe to the handler.
func (test *TestHandlerSuite) subscribe() {
	s := &test.Suite

	sub, err := zmq.NewSocket(zmq.SUB)
	s.Require().NoError(err)
	s.Require().NoError(sub.SetSubscribe(""))
	// It won't work if the trigger is using a tcp protocol.
	// For tcp protocol, use clientConfig.Url()
	url := config.ExternalUrl(test.config.BroadcastId, test.config.BroadcastPort)
	err = sub.Connect(url)

	s.Require().NoError(err)
	test.sub = sub
	test.subscribed = make(chan []string)
	test.closeClient = false

	test.poller = zmq.NewPoller()
	test.poller.Add(test.sub, zmq.POLLIN)

	for {
		if test.closeClient {
			break
		}

		polled, err := test.poller.Poll(time.Millisecond)
		s.Require().NoError(err)
		if len(polled) == 0 {
			continue
		}

		reply, err := test.sub.RecvMessage(0)
		s.Require().NoError(err)

		test.subscribed <- reply
	}
}

func (test *TestHandlerSuite) TearDownTest() {
	s := &test.Suite

	test.closeClient = true
	// wait a bit for closing a subscriber
	time.Sleep(time.Millisecond * 20)

	err := test.trigger.Close()
	s.Require().NoError(err)

	s.Require().NoError(test.pub.Close())

	// Wait a bit for the closing of publisher and trigger
	time.Sleep(time.Millisecond * 100)
}

// Test_14_Run runs the sync replier
func (test *TestHandlerSuite) Test_10_Run() {
	s := &test.Suite

	err := test.pub.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything works
	req := message.Request{Command: config.HandlerStatus, Parameters: key_value.Empty()}
	err = test.trigger.Submit(&req)
	s.Require().NoError(err)

	test.logger.Info("waiting for a message in the subscriber")
	subscribed := <-test.subscribed
	test.logger.Info("subscriber received a message", "message", subscribed)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHandler(t *testing.T) {
	suite.Run(t, new(TestHandlerSuite))
}
