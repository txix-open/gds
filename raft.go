package gds

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	"github.com/integration-system/gds/cluster"
	"github.com/integration-system/gds/config"
	"github.com/integration-system/gds/provider"
	"github.com/integration-system/gds/raft"
	"github.com/integration-system/gds/store"
	"github.com/integration-system/gds/ws"
	"github.com/integration-system/isp-etp-go"
	mux "github.com/integration-system/net-mux"
	"net"
	"net/http"
)

const (
	defaultWsConnectionReadLimit int64 = 4 << 20 // 4 MB
)

type RaftAdapter struct {
	Config        config.ClusterConfiguration
	RaftStore     *store.Store
	ClusterClient *cluster.Client

	HTTPServer *http.Server
	EtpServer  etp.Server
	muxer      mux.Mux

	logger hclog.Logger
}

func NewRaftAdapter(cfg config.ClusterConfiguration, handler store.CommandsHandler, typeProvider provider.TypeProvider, logger hclog.Logger) (*RaftAdapter, error) {
	adapter := &RaftAdapter{
		Config: cfg,
		logger: logger,
	}

	httpListener, raftListener, err := adapter.initMultiplexer(cfg.OuterAddress)
	if err != nil {
		return nil, fmt.Errorf("init mux: %v", err)
	}

	err = adapter.initRaft(raftListener, cfg, handler, typeProvider)
	if err != nil {
		return nil, fmt.Errorf("init raft: %v", err)
	}

	adapter.initWebsocket(context.Background(), httpListener)
	return adapter, nil
}

func (ra *RaftAdapter) initMultiplexer(address string) (net.Listener, net.Listener, error) {
	outerAddr, err := net.ResolveTCPAddr("tcp4", address)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve outer address: %v", err)
	}
	tcpListener, err := net.ListenTCP("tcp4", outerAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("create tcp transport: %v", err)
	}

	ra.muxer = mux.New(tcpListener)
	httpListener := ra.muxer.Match(mux.HTTP1())
	raftListener := ra.muxer.Match(mux.Any())

	go func() {
		if err := ra.muxer.Serve(); err != nil {
			ra.logger.Error(fmt.Sprintf("serve mux: %v", err))
		}
	}()
	return httpListener, raftListener, nil
}

func (ra *RaftAdapter) initWebsocket(ctx context.Context, listener net.Listener) {
	etpConfig := etp.ServerConfig{
		InsecureSkipVerify:  true,
		ConnectionReadLimit: defaultWsConnectionReadLimit,
	}
	etpServer := etp.NewServer(ctx, etpConfig)
	ws.NewSocketEventHandler(etpServer, ra.ClusterClient, ra.logger).SubscribeAll()

	httpMux := http.NewServeMux()
	httpMux.HandleFunc(cluster.WebsocketURLPath, etpServer.ServeHttp)
	httpServer := &http.Server{Handler: httpMux}
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			ra.logger.Error(fmt.Sprintf("http server closed: %v", err))
		}
	}()
	ra.EtpServer = etpServer
	ra.HTTPServer = httpServer
}

func (ra *RaftAdapter) initRaft(listener net.Listener, clusterCfg config.ClusterConfiguration,
	commandsHandler store.CommandsHandler, typeProvider provider.TypeProvider) error {
	raftStore := store.NewStore(commandsHandler, typeProvider)
	r, err := raft.NewRaft(listener, clusterCfg, raftStore, ra.logger)
	if err != nil {
		return fmt.Errorf("unable to create raft server: %v", err)
	}
	clusterClient := cluster.NewRaftClusterClient(r, ra.logger)

	if clusterCfg.BootstrapCluster {
		err = r.BootstrapCluster()
		if err != nil {
			return fmt.Errorf("bootstrap cluster: %v", err)
		}
	}
	ra.ClusterClient = clusterClient
	ra.RaftStore = raftStore
	return nil
}

func (ra *RaftAdapter) Shutdown(ctx context.Context) error {
	errs := new(multierror.Error)

	ra.EtpServer.Close()

	if err := ra.HTTPServer.Shutdown(ctx); err != nil {
		errs = multierror.Append(errs, fmt.Errorf("shutdown http server: %v", err))
	}

	if err := ra.ClusterClient.Shutdown(); err != nil {
		errs = multierror.Append(errs, fmt.Errorf("shutdown raft: %v", err))
	}

	if err := ra.muxer.Close(); err != nil {
		errs = multierror.Append(errs, fmt.Errorf("close listener: %v", err))
	}
	return errs.ErrorOrNil()
}
