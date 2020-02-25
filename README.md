# Gds
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/integration-system/gds?color=6b9ded&sort=semver)](https://github.com/integration-system/gds/releases)
[![Go API Reference](https://img.shields.io/badge/go-documentation-blue.svg?style=flat)](https://pkg.go.dev/github.com/integration-system/gds?tab=doc)
[![TODOs](https://badgen.net/https/api.tickgit.com/badgen/github.com/integration-system/gds)](https://www.tickgit.com/browse?repo=github.com/integration-system/gds)

A Raft-based embedded job scheduler for your Go projects.

# Features
- Disaster tolerance: if not all of the hosts are down, the job should be fired successfully.
- Validity: only one host runs each job.
- Schedule strategy.
- Automatic panic recovery.

# Examples
The [Examples folder](examples) contains a bunch of code samples you can look into.

# Custom job types
All jobs implementations have to satisfy `jobs/Job` interface. Internal job state such as counters should only change in `PostExecution` method. 
`PostExecution` method is called on each node in cluster in order to provide consistency.
`NextTriggerTime` method is idempotent and can be called many times, it have to return zero time.Time if job is finished. 
Job struct fields have to be public in order to marshal/unmarshal correctly or covered by Marshal/Unmarshal methods.

Note that all nodes in cluster must register the same JobExecutors because all nodes store full jobs state. 

# Planned
- Implement new Job types
- Add sync.Pool to cluster.prepareCommand()
- Library API improvements

# Development
Gds uses go modules and Go 1.13  
Checks before commit:
```bash
golangci-lint run
go test ./... -race
```

# License
MIT
