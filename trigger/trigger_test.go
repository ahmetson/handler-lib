package trigger

import (
	"github.com/ahmetson/client-lib"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/handler-lib/reactor"
	"github.com/ahmetson/log-lib"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestTriggerSuite struct {
	suite.Suite
	inprocHandler *Trigger
	inprocConfig  *config.Trigger
	inprocClient  *client.Socket
	logger        *log.Logger
}

// Todo test in-process and external types of controllers
// Todo test the business of the server
// Make sure that Account is set to five
// before each test
func (test *TestTriggerSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("trigger", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.inprocHandler = New()

	// Socket to talk to clients
	handlerConfig := config.NewInternalHandler(config.SyncReplierType, "test")
	triggerConfig, err := config.InternalTriggerAble(handlerConfig, config.PublisherType)
	s.Require().NoError(err)
	test.inprocConfig = triggerConfig

	// Setting the configuration
	// Setting the logger should be successful
	test.inprocHandler.SetConfig(test.inprocConfig)
	s.Require().NoError(test.inprocHandler.SetLogger(test.logger))
}

// Test_10_Config makes sure that configuration methods are running without any error.
//
// It doesn't validate the internal parameters.
// So it must validate them.
func (test *TestTriggerSuite) Test_10_Config() {
	s := &test.Suite

	// Testing config trigger-able
	handlerConfig, err := config.NewHandler(config.SyncReplierType, "test")
	s.Require().NoError(err)
	_, err = config.TriggerAble(handlerConfig, config.PublisherType)
	s.Require().NoError(err)
}

// Test_14_Run runs the trigger.
//
// The trigger creates a client that will subscribe for the messages to subscribe.
func (test *TestTriggerSuite) Test_14_Run() {
	s := &test.Suite

	err := test.inprocHandler.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything works
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), instance_manager.Running)
	s.Require().Equal(test.inprocHandler.Reactor.Status(), reactor.RUNNING)

	// Now let's close it
	err = test.inprocHandler.Close()

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything is closed
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), instance_manager.Idle)
	s.Require().Equal(test.inprocHandler.Reactor.Status(), reactor.CREATED)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestTrigger(t *testing.T) {
	suite.Run(t, new(TestTriggerSuite))
}
