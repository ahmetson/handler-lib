package replier

import (
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/common-lib/message"
	"github.com/ahmetson/handler-lib/config"
	"github.com/ahmetson/handler-lib/handler_manager"
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
	replier        *Replier
	handlerConfig  *config.Handler
	managingClient *zmq.Socket
	logger         *log.Logger
	routes         map[string]interface{}
}

func (test *TestHandlerSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("replier", true)
	test.Suite.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.replier = New()

	// Socket to talk to clients
	test.routes = make(map[string]interface{}, 2)
	test.routes["command_1"] = func(request message.Request) message.Reply {
		time.Sleep(time.Millisecond * 100)
		return request.Ok(request.Parameters.Set("id", request.Command))
	}
	test.routes["command_2"] = func(request message.Request) message.Reply {
		return request.Ok(request.Parameters.Set("id", request.Command))
	}

	err = test.replier.Route("command_1", test.routes["command_1"])
	s.Require().NoError(err)
	err = test.replier.Route("command_2", test.routes["command_2"])
	s.Require().NoError(err)

	test.handlerConfig = config.NewInternalHandler(config.ReplierType, "test")

	// Setting a logger should fail since we don't have a configuration set
	s.Require().Error(test.replier.SetLogger(test.logger))

	// Setting the configuration
	// Setting the logger should be successful
	test.replier.SetConfig(test.handlerConfig)
	s.Require().NoError(test.replier.SetLogger(test.logger))

	test.managingClient, err = zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	managerUrl := config.ManagerUrl(test.handlerConfig.Id)
	err = test.managingClient.Connect(managerUrl)
	s.Require().NoError(err)
}

func (test *TestHandlerSuite) TearDownTest() {
	s := &test.Suite

	err := test.managingClient.Close()
	s.Require().NoError(err)

	s.Require().NoError(test.replier.Close())

	// Wait a bit for closing
	time.Sleep(time.Millisecond * 200)
}

func (test *TestHandlerSuite) req(client *zmq.Socket, request message.Request) message.Reply {
	s := &test.Suite

	reqStr, err := request.String()
	s.Require().NoError(err)

	_, err = client.SendMessage(reqStr)
	s.Require().NoError(err)

	raw, err := client.RecvMessage(0)
	s.Require().NoError(err)

	reply, err := message.ParseReply(raw)
	s.Require().NoError(err)

	return reply
}

// Test_10_Run runs the sync replier
func (test *TestHandlerSuite) Test_10_Run() {
	s := &test.Suite

	err := test.replier.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything works
	req := message.Request{Command: config.HandlerStatus, Parameters: key_value.Empty()}
	reply := test.req(test.managingClient, req)
	s.Require().True(reply.IsOK())

	status, err := reply.Parameters.GetString("status")
	s.Require().NoError(err)
	s.Require().Equal(handler_manager.Ready, status)

	// By default, the handler creates a socket.
	// Trying to add a new socket, it will throw an error
	s.Require().Len(test.replier.base.InstanceManager.Instances(), 1)

	instanceAmount := test.replier.MaxInstanceAmount()

	// Adding a new instance to make reach the cap
	for i := 1; i < int(instanceAmount); i++ {
		req.Command = config.AddInstance
		reply = test.req(test.managingClient, req)
		s.Require().True(reply.IsOK())
	}

	time.Sleep(time.Millisecond * 200)

	// Trying to add more instance must fail
	req.Command = config.AddInstance
	reply = test.req(test.managingClient, req)
	s.Require().False(reply.IsOK())
}

// Test_11_Request tests that external clients send the message to the instance
func (test *TestHandlerSuite) Test_11_Request() {
	s := &test.Suite

	err := test.replier.Start()
	s.Require().NoError(err)

	// Wait a bit for initialization
	time.Sleep(time.Millisecond * 100)

	// Make sure that everything works
	req := message.Request{Command: config.HandlerStatus, Parameters: key_value.Empty()}
	reply := test.req(test.managingClient, req)
	s.Require().True(reply.IsOK())

	status, err := reply.Parameters.GetString("status")
	s.Require().NoError(err)
	s.Require().Equal(handler_manager.Ready, status)

	// By default, the handler creates a socket.
	// Trying to add a new socket, it will throw an error
	s.Require().Len(test.replier.base.InstanceManager.Instances(), 1)

	instanceAmount := test.replier.MaxInstanceAmount()
	test.logger.Info("run", "current", len(test.replier.base.InstanceManager.Instances()), "max", instanceAmount)

	// Adding a new instance to make reach the cap
	for i := 1; i < 3; i++ {
		req.Command = config.AddInstance
		reply = test.req(test.managingClient, req)
		s.Require().True(reply.IsOK())
	}

	// Wait until threads are live
	time.Sleep(time.Millisecond * 100)

	// Let's create a client
	client, err := zmq.NewSocket(zmq.REQ)
	s.Require().NoError(err)
	externalUrl := config.ExternalUrl(test.handlerConfig.Id, test.handlerConfig.Port)
	err = client.Connect(externalUrl)
	s.Require().NoError(err)

	req = message.Request{Command: "command_1", Parameters: key_value.Empty()}
	reply = test.req(client, req)
	s.Require().True(reply.IsOK())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestHandler(t *testing.T) {
	suite.Run(t, new(TestHandlerSuite))
}