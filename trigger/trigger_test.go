package trigger

import (
	"github.com/ahmetson/client-lib"
	clientConfig "github.com/ahmetson/client-lib/config"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/instance_manager"
	"github.com/ahmetson/handler-lib/reactor"
	"github.com/ahmetson/log-lib"
	"github.com/stretchr/testify/suite"
	"slices"
	"testing"
	"time"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestTriggerSuite struct {
	suite.Suite
	tcpHandler    *Trigger
	inprocHandler *Trigger
	tcpConfig     *config.Trigger
	inprocConfig  *config.Trigger
	tcpClient     *client.Socket
	inprocClient  *client.Socket
	logger        *log.Logger
}

// Todo test in-process and external types of controllers
// Todo test the business of the server
// Make sure that Account is set to five
// before each test
func (test *TestTriggerSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("handler", false)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.tcpHandler = New()
	test.inprocHandler = New()

	// Socket to talk to clients
	handlerConfig := config.NewInternalHandler(config.SyncReplierType, "test")
	triggerConfig, err := config.InternalTriggerAble(handlerConfig, config.PublisherType)
	s.Require().NoError(err)
	test.inprocConfig = triggerConfig

	handlerConfig, err = config.NewHandler(config.SyncReplierType, "test")
	s.Require().NoError(err)
	triggerConfig, err = config.TriggerAble(handlerConfig, config.PublisherType)
	s.Require().NoError(err)
	test.tcpConfig = triggerConfig

	// Setting a logger should fail since we don't have a configuration set
	s.Require().Error(test.inprocHandler.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.inprocHandler.SetConfig(test.inprocConfig)
	s.Require().NoError(test.inprocHandler.SetLogger(test.logger))

	// Setting the parameters of the Tcp Handler
	test.tcpHandler.SetConfig(test.tcpConfig)
	s.Require().NoError(test.tcpHandler.SetLogger(test.logger))
}

// Test_11_Deps tests setting of the route dependencies
func (test *TestTriggerSuite) Test_11_Deps() {
	s := &test.Suite

	// Handler must not have dependencies yet
	s.Require().Empty(test.inprocHandler.DepIds())
	s.Require().Empty(test.tcpHandler.DepIds())

	depIds := test.inprocHandler.DepIds()
	s.Require().Len(depIds, 3)
	s.Require().EqualValues([]string{"dep_1", "dep_2", "dep_3"}, depIds)

}

// Test_12_DepConfig tests setting of the dependency configurations
func (test *TestTriggerSuite) Test_12_DepConfig() {
	s := &test.Suite

	// No dependency Config should be given
	depIds := test.inprocHandler.DepIds()
	//AddDepByService
	for _, id := range depIds {
		s.Require().False(test.inprocHandler.AddedDepByService(id))
	}

	// Adding the dependencies
	for _, id := range depIds {
		depConfig := &clientConfig.Client{
			Id:         id,
			ServiceUrl: "github.com/ahmetson/" + id,
			Port:       0,
		}

		s.Require().NoError(test.inprocHandler.AddDepByService(depConfig))
	}

	// There should be dependency configurations now
	for _, id := range depIds {
		s.Require().True(test.inprocHandler.AddedDepByService(id))
	}

	// trying to add the configuration for the dependency that doesn't exist should fail
	depId := "not_exist"
	s.Require().False(slices.Contains(depIds, depId))
	depConfig := &clientConfig.Client{
		Id:         depId,
		ServiceUrl: "github.com/ahmetson/" + depId,
		Port:       0,
	}
	s.Require().Error(test.inprocHandler.AddDepByService(depConfig))

	// Trying to add the configuration that was already added should fail
	depId = depIds[0]
	depConfig = &clientConfig.Client{
		Id:         depId,
		ServiceUrl: "github.com/ahmetson/" + depId,
		Port:       0,
	}
	s.Require().Error(test.inprocHandler.AddDepByService(depConfig))
}

// Test_13_InstanceManager tests setting of the instance Manager and then listening to it.
func (test *TestTriggerSuite) Test_13_InstanceManager() {
	s := &test.Suite

	// the instance Manager requires
	s.Require().NotNil(test.inprocHandler.InstanceManager)

	// It should be idle
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), instance_manager.Idle)
	s.Require().Empty(test.inprocHandler.InstanceManager.Instances())

	// Running instance Manager
	go test.inprocHandler.RunInstanceManager()

	// Waiting a bit for instance Manager initialization
	time.Sleep(time.Millisecond * 2000)

	// Instance Manager should be running
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), instance_manager.Running)
	s.Require().Len(test.inprocHandler.InstanceManager.Instances(), 1)

	// Let's send the close signal
	s.Require().NoError(test.inprocHandler.Close())

	// Waiting a bit for instance Manager closing
	time.Sleep(time.Millisecond * 10)

	// Check that Instance Manager is not running
	s.Require().Equal(test.inprocHandler.InstanceManager.Status(), instance_manager.Idle)
	s.Require().Empty(test.inprocHandler.InstanceManager.Instances())
}

// Test_14_Run runs the handler.
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
