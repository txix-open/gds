package main

import (
	"context"
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/integration-system/gds"
	"github.com/integration-system/gds/config"
	"github.com/integration-system/gds/jobs"
)

type testStruct struct {
	Data string
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "String()"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	bootstrap := false
	bench := false
	addr := ""
	var peers arrayFlags

	flag.BoolVar(&bootstrap, "bootstrap", false, "")
	flag.BoolVar(&bench, "bench", false, "")
	flag.StringVar(&addr, "addr", "", "")
	flag.Var(&peers, "peer", "")
	flag.Parse()
	peers = append(peers, addr)

	cfg := config.ClusterConfiguration{
		InMemory: true,
		// BootstrapCluster must be true only on one node in cluster.
		BootstrapCluster: bootstrap,
		OuterAddress:     addr,
		// Peers contains all peers addresses
		Peers: peers,
	}
	scheduler, err := gds.NewScheduler(cfg)
	if err != nil {
		panic(err)
	}

	// newJob is used to restore type information between startups
	newJob := func() jobs.Job {
		// Fields in testStruct have to be public in order to unmarshal correctly
		return &jobs.OneTimeJob{Data: new(testStruct)}
	}
	jobType := "test_job"

	jobExecutor := func(job jobs.Job) error {
		oneTimeJob := job.(*jobs.OneTimeJob)
		payload := oneTimeJob.Data.(*testStruct)
		fmt.Printf("processed job with data %s\n", payload.Data)
		return nil
	}

	// Register job type
	scheduler.RegisterExecutor(jobType, jobExecutor, newJob)

	// Wait until there will be leader in cluster. until that all ScheduleJob calls will fail
	scheduler.WaitCluster(context.Background())

	if !bench {
		select {}
	}

	var counter uint32 = 0
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				val := atomic.SwapUint32(&counter, 0)
				fmt.Printf("%d req/sec\n", val)
			}
		}
	}()

	i := 0
	for {
		data := fmt.Sprintf("somedata-%d", i)
		jobPayload := testStruct{Data: data}
		key := fmt.Sprintf("key-%d", i)
		job, err := jobs.NewOneTimeJob(jobType, key, time.Now().Add(60*time.Minute), jobPayload)
		if err != nil {
			panic(err)
		}

		err = scheduler.ScheduleJob(job)
		if err != nil {
			panic(err)
		}

		atomic.AddUint32(&counter, 1)
		i++
	}
}
