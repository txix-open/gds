package gds

import (
	"context"
	"errors"
	"fmt"
	"github.com/integration-system/gds/config"
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/store"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testStruct struct {
	Name       string
	Number     int
	AssignedID int
}

func TestScheduler_InsertDelete(t *testing.T) {
	const (
		instancesNumber = 3
		startPort       = 8110
	)
	a := assert.New(t)
	schedulers, _ := setup(t, instancesNumber, startPort)
	var jobsFinished int64 = 0

	jobExecutor := func(job jobs.Job) error {
		oneTimeJob := job.(*jobs.OneTimeJob)
		_ = oneTimeJob.Data.(*testStruct)
		atomic.AddInt64(&jobsFinished, 1)
		return nil
	}
	newJob := func() jobs.Job { return &jobs.OneTimeJob{Data: new(testStruct)} }

	jobType := "simple_job"

	for i := 0; i < instancesNumber; i++ {
		scheduler := schedulers[i]
		scheduler.RegisterExecutor(jobType, jobExecutor, newJob)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a.True(schedulers[0].WaitCluster(ctx))

	key := "key-123"
	jobPayload := testStruct{Name: "name1", Number: 123}
	job, err := jobs.NewOneTimeJob(jobType, key, time.Now().Add(500*time.Millisecond), jobPayload)
	a.NoError(err)

	err = schedulers[0].ScheduleJob(job)
	a.NoError(err)

	time.Sleep(200 * time.Millisecond)
	jobInfo, err := schedulers[1].GetJob(key)
	a.NoError(err)
	a.True(jobInfo.State == jobs.StateScheduled)

	err = schedulers[1].DeleteJob(key)
	a.NoError(err)

	time.Sleep(1 * time.Second)
	_, err = schedulers[1].GetJob(key)
	a.True(err == store.ErrJobNotFound)

	a.EqualValues(0, jobsFinished)
}

func TestScheduler_InsertRemovePeerAndReschedule(t *testing.T) {
	const (
		instancesNumber = 3
		startPort       = 8115
	)
	a := assert.New(t)
	schedulers, configs := setup(t, instancesNumber, startPort)
	var jobsFinished int64 = 0

	jobExecutor := func(job jobs.Job) error {
		oneTimeJob := job.(*jobs.OneTimeJob)
		_ = oneTimeJob.Data.(*testStruct)
		atomic.AddInt64(&jobsFinished, 1)
		return nil
	}
	newJob := func() jobs.Job { return &jobs.OneTimeJob{Data: new(testStruct)} }

	jobType := "simple_job"

	for i := 0; i < instancesNumber; i++ {
		scheduler := schedulers[i]
		scheduler.RegisterExecutor(jobType, jobExecutor, newJob)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	a.True(schedulers[0].WaitCluster(ctx))

	key := "key-123"
	jobPayload := testStruct{Name: "name1", Number: 123}
	job, err := jobs.NewOneTimeJob(jobType, key, time.Now().Add(4*time.Second), jobPayload)
	a.NoError(err)

	err = schedulers[2].ScheduleJob(job)
	a.NoError(err)

	time.Sleep(200 * time.Millisecond)
	jobInfo, err := schedulers[1].GetJob(key)
	a.NoError(err)
	a.Equal(jobs.StateScheduled, jobInfo.State)
	assignedPeerID := jobInfo.AssignedPeerID
	if !a.NotEqual("", assignedPeerID) {
		a.FailNow("didn't schedule job")
	}

	schedulerN := -1
	activeSchedulerN := instancesNumber - 1
	for i, cfg := range configs {
		if cfg.OuterAddress == assignedPeerID {
			schedulerN = i
			break
		}
		activeSchedulerN = i
	}
	if !a.NotEqual(-1, schedulerN) {
		a.FailNow("didn't elect new leader")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	err = schedulers[schedulerN].Shutdown(ctx2)
	a.NoError(err)

	time.Sleep(4000 * time.Millisecond)
	jobInfo, err = schedulers[activeSchedulerN].GetJob(key)
	a.NoError(err)
	a.NotEqual(jobInfo.AssignedPeerID, assignedPeerID)
	a.NotEqual(jobInfo.AssignedPeerID, "")

	schedulers[activeSchedulerN].(*scheduler).store.VisitReadonlyState(func(state store.ReadonlyState) {
		a.Len(state.GetPeers(), 2)
	})

	a.EqualValues(1, jobsFinished)
}

func TestScheduler_OneNode(t *testing.T) {
	const (
		instancesNumber = 1
		startPort       = 8101
	)
	genericTestSchedulerScheduleJob(t, instancesNumber, startPort)
}

func TestScheduler_ManyNodes(t *testing.T) {
	const (
		instancesNumber = 5
		startPort       = 8103
	)
	genericTestSchedulerScheduleJob(t, instancesNumber, startPort)
}

var scheduleJobBenchStartPort = 8200

func BenchmarkSchedulerScheduleJob(b *testing.B) {
	const (
		instancesNumber = 3
	)
	defer func() {
		scheduleJobBenchStartPort += instancesNumber
	}()
	a := assert.New(b)
	schedulers, _ := setup(b, instancesNumber, scheduleJobBenchStartPort)

	jobExecutor := func(job jobs.Job) error {
		oneTimeJob := job.(*jobs.OneTimeJob)
		_ = oneTimeJob.Data.(*testStruct)
		return nil
	}
	newJob := func() jobs.Job { return &jobs.OneTimeJob{Data: new(testStruct)} }

	jobType := "simple_job"
	for i := 0; i < instancesNumber; i++ {
		scheduler := schedulers[i]
		scheduler.RegisterExecutor(jobType, jobExecutor, newJob)
		defer func(schedulers []Scheduler, i int) {
			go func() {
				_ = schedulers[i].Shutdown(context.Background())
			}()
		}(schedulers, i)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a.True(schedulers[0].WaitCluster(ctx))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		jobPayload := testStruct{Name: "name1", Number: i}
		key := fmt.Sprintf("key-%d", i)
		job, err := jobs.NewOneTimeJob(jobType, key, time.Now().Add(50*time.Second), jobPayload)
		a.NoError(err)

		schedulerNumber := i % instancesNumber

		b.StartTimer()
		err = schedulers[schedulerNumber].ScheduleJob(job)
		b.StopTimer()

		a.NoError(err)
		b.StartTimer()
	}
}

func genericTestSchedulerScheduleJob(t *testing.T, instancesNumber, startPort int) {
	const jobsCount = 7000
	schedulers, _ := setup(t, instancesNumber, startPort)
	a := assert.New(t)
	wg := new(sync.WaitGroup)
	var jobsFinished int64 = 0

	jobsDistibutedByPeers := make(map[int]*int64, instancesNumber)

	makeJobExecutor := func(_ int) func(job jobs.Job) error {
		return func(job jobs.Job) error {
			defer wg.Done()
			defer func() {
				err := recover()
				if err != nil {
					panic(err)
				}
			}()
			oneTimeJob := job.(*jobs.OneTimeJob)
			//payload := oneTimeJob.Data.(*testStruct)
			_ = oneTimeJob.Data.(*testStruct)

			// For debugging
			//fmt.Printf("%d\n", payload.Number)
			//log.Printf("DO job %s, number %d, %s from peer %d", oneTimeJob.Key(), payload.Number, payload.Name, i)
			atomic.AddInt64(&jobsFinished, 1)
			//atomic.AddInt64(jobsDistibutedByPeers[i], 1)
			//fmt.Fprint(ioutil.Discard, schedulers)
			return nil
		}
	}

	newJob := func() jobs.Job { return &jobs.OneTimeJob{Data: new(testStruct)} }
	jobType := "simple_job"

	for i := 0; i < instancesNumber; i++ {
		ii := i
		var val int64 = 0
		jobsDistibutedByPeers[i] = &val
		scheduler := schedulers[i]
		scheduler.RegisterExecutor(jobType, makeJobExecutor(ii), newJob)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a.True(schedulers[instancesNumber-1].WaitCluster(ctx))
	wg.Add(jobsCount)
	for i := 0; i < jobsCount; i++ {
		schedulerNumber := i % instancesNumber

		name := fmt.Sprintf("name-%d", i)
		jobPayload := testStruct{Name: name, Number: i, AssignedID: schedulerNumber}
		key := fmt.Sprintf("key-%d", i)
		job, err := jobs.NewOneTimeJob(jobType, key, time.Now().Add(time.Second), jobPayload)
		a.NoError(err)

		err = schedulers[schedulerNumber].ScheduleJob(job)
		a.NoError(err)
	}

	err := wait(wg, 3*time.Second)
	a.EqualValues(jobsCount, atomic.LoadInt64(&jobsFinished))
	if a.NoError(err) {
		fmt.Println("finished, jobs distributed by peers:")
		for i, val := range jobsDistibutedByPeers {
			fmt.Printf("%d peer: %d jobs\n", i, *val)
		}
	}
}

func setup(t testing.TB, instancesNumber, startPort int) ([]Scheduler, []config.ClusterConfiguration) {
	schedulers := make([]Scheduler, instancesNumber)
	configs := make([]config.ClusterConfiguration, instancesNumber)

	addrs := make([]string, instancesNumber)
	for i := 0; i < instancesNumber; i++ {
		port := startPort + i
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		cfg := config.ClusterConfiguration{
			JobExecutionTimeout: -1,
			InMemory:            true,
			DataDir:             "",
			BootstrapCluster:    false,
			OuterAddress:        addr,
			Peers:               nil,
		}

		addrs[i] = addr
		configs[i] = cfg
	}

	configs[0].BootstrapCluster = true

	for i := 0; i < instancesNumber; i++ {
		cfg := configs[i]
		cfg.Peers = addrs
		configs[i] = cfg

		scheduler, err := NewScheduler(cfg)
		assert.NoError(t, err)

		schedulers[i] = scheduler
	}
	return schedulers, configs
}

func wait(wg *sync.WaitGroup, duration time.Duration) error {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case <-time.After(duration):
		return errors.New("waiting timeout exceeded")
	case <-ch:
	}

	return nil
}
