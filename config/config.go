package config

import (
	"time"
)

type ClusterConfiguration struct {
	// timeout < 0 means no timeout. If timeout == 0, DefaultJobExecutionTimeout will be used.
	JobExecutionTimeout time.Duration
	// Store all scheduler information in memory. If set to false, DataDir param is required.
	InMemory bool
	// BootstrapCluster must be true only on one node in cluster.
	BootstrapCluster bool
	// Dir used to store all raft data.
	DataDir string
	// Peer OuterAddress in format address:port
	OuterAddress string
	// Addresses of peers in cluster including OuterAddress in format address:port
	Peers []string
}
