package cluster

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
	etp "github.com/integration-system/isp-etp-go/client"
	log "github.com/integration-system/isp-log"
	"net"
	"net/http"
	"net/url"
	"time"
)

type SocketLeaderClient struct {
	client    etp.Client
	url       string
	globalCtx context.Context
	cancel    context.CancelFunc
}

func (c *SocketLeaderClient) Ack(data []byte, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(c.globalCtx, timeout)
	defer cancel()
	response, err := c.client.EmitWithAck(ctx, ApplyCommandEvent, data)
	return response, err
}

func (c *SocketLeaderClient) Dial(timeout time.Duration) error {
	backOff := backoff.NewExponentialBackOff()
	backOff.MaxElapsedTime = timeout
	bf := backoff.WithContext(backOff, c.globalCtx)
	dial := func() error {
		return c.client.Dial(c.globalCtx, c.url)
	}
	return backoff.Retry(dial, bf)
}

func (c *SocketLeaderClient) Close() {
	c.cancel()
	err := c.client.Close()
	if err != nil {
		log.Warnf(0, "leader client close err: %v", err)
	}
	log.Debug(0, "leader client connection closed")
}

func NewSocketLeaderClient(leaderAddr, localID string) *SocketLeaderClient {
	etpConfig := etp.Config{
		HttpClient: http.DefaultClient,
	}
	client := etp.NewClient(etpConfig)
	leaderClient := &SocketLeaderClient{
		client: client,
		url:    getURL(leaderAddr, localID),
	}
	ctx, cancel := context.WithCancel(context.Background())
	leaderClient.globalCtx = ctx
	leaderClient.cancel = cancel

	leaderClient.client.OnDisconnect(func(err error) {
		log.WithMetadata(map[string]interface{}{
			"leaderAddr": leaderAddr,
		}).Warn(0, "leader client disconnected")
	})

	leaderClient.client.OnError(func(err error) {
		log.Warnf(0, "leader client on error: %v", err)
	})
	leaderClient.client.OnConnect(func() {
		log.Debug(0, "leader client connected")
	})
	return leaderClient
}

func getURL(address, localID string) string {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		panic(err) // must never occurred
	}

	params := url.Values{}
	params.Add(PeerIDGetParam, localID)
	return fmt.Sprintf("ws://%s:%d%s?%s", addr.IP.String(), addr.Port, WebsocketURLPath, params.Encode())
}
