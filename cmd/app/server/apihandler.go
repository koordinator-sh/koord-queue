package app

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/kube-queue/kube-queue/pkg/controller"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"net"
	"net/http"
	"time"
)

const (
	DefaultServingPort = 19876
)

type logAdapter struct{}

func (logAdapter) Write(d []byte) (int, error) {
	klog.Info(string(d))
	return len(d), nil
}

func init() {
	gin.DefaultWriter = logAdapter{}
	gin.DefaultErrorWriter = logAdapter{}
	gin.DisableConsoleColor()
}

func ServeAPIHandlers(ctx context.Context, controller *controller.Controller) {
	engine := gin.Default()
	installAPIHandlers(engine, controller)
	controller.GetFramework().RegisterApiHandler(engine)

	listener, _, _ := createListener()
	server := &http.Server{
		Addr:           listener.Addr().String(),
		Handler:        engine,
		MaxHeaderBytes: 1 << 20,
	}
	runServer(server, listener, 0, ctx.Done())
}

func createListener() (net.Listener, int, error) {
	network := "tcp"
	bindAddress := net.ParseIP("0.0.0.0")
	addr := net.JoinHostPort(bindAddress.String(), fmt.Sprintf("%d", DefaultServingPort))

	if len(network) == 0 {
		network = "tcp"
	}
	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to listen on %v: %v", addr, err)
	}

	// get port
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		ln.Close()
		return nil, 0, fmt.Errorf("invalid listen address: %q", ln.Addr().String())
	}

	return ln, tcpAddr.Port, nil
}

func runServer(
	server *http.Server,
	ln net.Listener,
	shutDownTimeout time.Duration,
	stopCh <-chan struct{},
) (<-chan struct{}, error) {
	if ln == nil {
		return nil, fmt.Errorf("listener must not be nil")
	}

	// Shutdown server gracefully.
	stoppedCh := make(chan struct{})
	go func() {
		defer close(stoppedCh)
		<-stopCh
		ctx, cancel := context.WithTimeout(context.Background(), shutDownTimeout)
		server.Shutdown(ctx)
		cancel()
	}()

	go func() {
		defer utilruntime.HandleCrash()

		err := server.Serve(ln)
		msg := fmt.Sprintf("Stopped listening on %s", ln.Addr().String())
		select {
		case <-stopCh:
			klog.Info(msg)
		default:
			panic(fmt.Sprintf("%s due to error: %v", msg, err))
		}
	}()

	return stoppedCh, nil
}
