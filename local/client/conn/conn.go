package conn

import (
	"github.com/scootdev/scoot/local/protocol"
	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

// A Dialer can dial a connection to the Scoot server.
// It's useful to have this as a separate interface so you can wait to
// connect until you need the connection. This allows clients to do client-side
// only operations (e.g., printing help) without erroring if the server is down.
type Dialer interface {
	Dial() (Conn, error)
}

type Conn interface {
	Echo(arg string) (string, error)

	// TODO(dbentley): this feels weird. We shouldn't expose our internal
	// API to the client. But it also feels weird to copy everything.
	// A Conn is also a Runner
	runner.Runner

	Close() error
}

func UnixDialer() (Dialer, error) {
	socketPath, err := protocol.LocateSocket()
	if err != nil {
		return nil, err
	}
	return &dialer{socketPath}, nil
}

type dialer struct {
	socketPath string
}

func dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("unix", addr, timeout)
}

func (d *dialer) Dial() (Conn, error) {
	log.Println("Dialing ", d.socketPath)
	c, err := grpc.Dial(d.socketPath, grpc.WithInsecure(), grpc.WithDialer(dial))
	if err != nil {
		return nil, err
	}
	client := protocol.NewLocalScootClient(c)
	return &conn{c, client}, nil
}

type conn struct {
	conn   *grpc.ClientConn
	client protocol.LocalScootClient
}

func (c *conn) Echo(arg string) (string, error) {
	r, err := c.client.Echo(context.Background(), &protocol.EchoRequest{Ping: arg})
	if err != nil {
		return "", err
	}
	return r.Pong, nil
}

func (c *conn) Run(cmd *runner.Command) (*runner.ProcessStatus, error) {
	req := &protocol.Command{}
	req.Argv = cmd.Argv
	req.Env = cmd.EnvVars
	req.Timeout = int64(cmd.Timeout)

	r, err := c.client.Run(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return protocol.ToRunnerStatus(r), nil
}

func (c *conn) Status(run runner.RunId) (*runner.ProcessStatus, error) {
	r, err := c.client.Status(context.Background(), &protocol.StatusQuery{RunId: string(run)})
	if err != nil {
		return nil, err
	}
	return protocol.ToRunnerStatus(r), nil
}

func (c *conn) Close() error {
	return c.conn.Close()
}