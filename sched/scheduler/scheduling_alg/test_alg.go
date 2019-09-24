/*
Scheduling algorithm evaluation framework.
The objective is to determine if a scheduling algorithm provides better throughput than
the current production scheduling algorithm.  The framework runs on a standalone machine
running the scheduler with the new algorithm and emulating the production delays for each
task.  Its output is a comparison of the shadow job duration to the original production
job duration and intermediate stats on tasks running and waiting.

Input parameters define the set of jobs and starting build numbers to shadow and the length
of time (minutes) to include in the shadow window.

The framework builds a set of job definitions using ciui logs from the builds that ran within
that window.  Each job definition contains the same number of tasks as its original job, the elapsed
time for each original task and the job's start time relative to the test window.
 */
package scheduling_alg

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/scheduler"
)

type externalDeps struct {
	// external components used by scheduler
	initialCl       []cluster.Node
	clUpdates       chan []cluster.NodeUpdate
	sc              saga.SagaCoordinator
	fakeRunners     func(cluster.Node) runner.Service
	nodeToWorkerMap map[string]runner.Service
	statsRegistry   stats.StatsRegistry
	statsReceiver   stats.StatsReceiver
	latchTime       time.Duration
	statsCancelFn   func()
}

/*
fake cluster
*/
type testCluster struct {
	ch    chan []cluster.NodeUpdate
	nodes []cluster.Node
}

type timeSummary struct {
	// structure for storing summary info about the job
	buildUrl     string	       // the original build url
	prodDuration time.Duration // the production duration from the log
	testDuration time.Duration // the build's duration in the test
	testStart time.Time        // when the build was started in the test
}

type jobFinishTimes struct {
	// finish times for all the jobs.  Values are collected in watchForAllDone()
	// goroutine then passed back to the main process via jobTimesCh
	times map[string] time.Time
}

type SchedulingAlgTester struct {
	extDeps *externalDeps
	statsFileName string
}

func MakeSchedulingAlgTester() *SchedulingAlgTester {
	tDir := fmt.Sprintf("%sCloudExec", os.TempDir())
	if _, err := os.Stat(tDir); os.IsNotExist(err) {
		os.Mkdir(tDir, 0777)
	}

	return &SchedulingAlgTester{
		statsFileName: fmt.Sprintf("%s/newAlgStats.csv", tDir),
	}
}

func (st *SchedulingAlgTester) RunTest(jobDefsMap map[int][]*sched.JobDefinition, pRatios []int, clusterSize int) error {
	st.extDeps = st.getExternals(clusterSize)

	comparisonMap := make(map[string] *timeSummary)
	config := st.getTestConfig()
	s := scheduler.NewStatefulScheduler(
		st.extDeps.initialCl,
		st.extDeps.clUpdates,
		st.extDeps.sc,
		st.extDeps.fakeRunners,
		config,
		st.extDeps.statsReceiver,
	)

	s.SchedAlg = scheduler.MakePriorityBasedAlg(pRatios[:])

	rm := st.getRequestorMap(jobDefsMap)

	sc := s.GetSagaCoord()

	// start a go routine printing the stats
	stopStatsCh := make(chan bool)
	go st.printStats(st.extDeps, stopStatsCh, rm)

	// set up goroutine picking up job completion times
	addJobIdCh := make(chan string, len(jobDefsMap))  // pass job ids to watchForAllDone via this channel
	jobTimesCh := make(chan jobFinishTimes) // completion time for all jobs are returned on this channel
	allJobsStartedCh := make(chan bool)     // used this channel to tell the watchForAllDone that it has all job ids
	go st.watchForAllDone(addJobIdCh, allJobsStartedCh, jobTimesCh, sc)

	// initialize structures for running the jobs
	shadowStart := time.Now()
	jobIds := make([]string, 0)
	// sort the job map so we run them in ascending time order
	keys := make([]int, 0)
	for k, _ := range jobDefsMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	// now start running the jobs at the same frequency that they were run in production
	log.Warnf("%s: Starting %d jobs.", shadowStart.Format(time.RFC3339), len(jobDefsMap))
	for _, key := range keys {
		jobDefs := jobDefsMap[key]
		for _, jobDef := range jobDefs {
			// pause to simulate the frequency in which the jobs arrived in production
			deltaFromStart, e := st.extractWaitDurationFromJobDef(jobDef)
			if e != nil {
				return fmt.Errorf("Couldn't get deltaStartDuration:%s, skipping job", e.Error())
			}
			n := time.Now()
			startTime := shadowStart.Add(deltaFromStart)
			if startTime.After(n) {
				time.Sleep(startTime.Sub(n)) // this pause emulates the jobs' run frequency
			}

			// give the job to the scheduler
			id, err := s.ScheduleJob(*jobDef)
			if err != nil {
				return fmt.Errorf("Expected job to be Scheduled Successfully %v", err)
			}
			if id == "" {
				return fmt.Errorf("Expected successfully scheduled job to return non empty job string!")
			}

			addJobIdCh <- id  // give the job id to the watchForAllDone goroutine
			jobIds = append(jobIds, id)

			comparisonMap[id], err = st.makeTimeSummary(jobDef) // record job start time
			if err != nil {
				return fmt.Errorf("error extracting prod time from %s. %s", jobDef.Tag, err.Error())
			}
		}
	}
	allJobsStartedCh <- true  // tell watchForAllDone that it will not get any more job ids

	log.Warn("Waiting for final tasks to complete\n")
	finishTimes := <-jobTimesCh // wait for go routine collecting job times to report them back

	// shut down stats
	stopStatsCh <- true    // stop the timed stats collection/printing

	st.writeStatsToFile(st.extDeps, rm) // write the final stats
	st.extDeps.statsCancelFn()  // stop stats collectors

	st.printFinalComparison(finishTimes, comparisonMap)
	return nil
}

/*
watch for jobs completing.  Record finish times. When all the jobs
have finished put the finished times on an all done channel
 */
func (st *SchedulingAlgTester) watchForAllDone(addJobIdCh chan string, allJobsStartedCh chan bool, jobTimesCh chan jobFinishTimes, sc saga.SagaCoordinator) {
	jobFinishTimes := jobFinishTimes{times:make(map[string] time.Time)}
	jobIds := make([]string, 0)
	allDone := false
	finalCnt := -1
	haveAllIds := false
	for ! allDone  {
		if ! haveAllIds {
			// pull all newly added jobs id from the addJobIdCh, and recognize when
			// we've got all the job ids
			keepLooping := true
			for keepLooping {
				select {
				case id := <- addJobIdCh:
					jobIds = append(jobIds, id)
				case <-allJobsStartedCh:
					finalCnt = len(jobIds)
					haveAllIds = true
					keepLooping = false
				default:  // we've emptied the channel
					keepLooping = false
				}
			}
		}

		// look for completed jobs, record their finish times
		for _, id := range jobIds {
			if _, ok := jobFinishTimes.times[id]; !ok {
				// we haven't seen the job finish yet, check its state
				s, _ := sc.GetSagaState(id)
				if s.IsSagaCompleted() {
					// the job is newly finished, record its time
					jobFinishTimes.times[id] = time.Now()
				}
			}
			if finalCnt > 0 && len(jobFinishTimes.times) == finalCnt {
				allDone = true
				break
			}
		}
		time.Sleep(3 * time.Second)
	}

	jobTimesCh <- jobFinishTimes
}

func (st *SchedulingAlgTester) makeTimeSummary(jobDef *sched.JobDefinition) (*timeSummary, error) {
	re := regexp.MustCompile("url:(.*), elapsedMin:([0-9]+)")
	m := re.FindStringSubmatch(jobDef.Tag)
	buildUrl := m[1]
	prodDurationStr := m[2]
	prodDuration, e := strconv.Atoi(prodDurationStr)
	if e != nil {
		return nil, fmt.Errorf("couldn't parse elapsedMin value:%s, %s", prodDurationStr,e.Error() )
	}
	return &timeSummary{
		buildUrl: buildUrl,
		prodDuration: time.Duration(prodDuration) * time.Minute,
		testStart:		time.Now(),
	}, nil
}

/*
extract the time from Basis field
 */
func (st *SchedulingAlgTester) extractWaitDurationFromJobDef(jobDef *sched.JobDefinition) (time.Duration, error) {
	d, e := strconv.Atoi(jobDef.Basis)
	if e != nil {
		return time.Duration(0), fmt.Errorf("couldn't parse duration from job def basis:%s", e.Error())
	}
	return time.Duration(d), nil
}

func (st *SchedulingAlgTester) getExternals (clusterSize int) *externalDeps {

	cl := st.makeTestCluster(clusterSize)
	statsReg := stats.NewFinagleStatsRegistry()
	latchTime := 30 * time.Second
	statsRec, cancelFn:= stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsReg }, latchTime)


	return &externalDeps{
		initialCl: cl.nodes,
		clUpdates: cl.ch,
		sc:        sagalogs.MakeInMemorySagaCoordinatorNoGC(),
		fakeRunners: func(n cluster.Node) runner.Service {
			return makeFakeWorker(n)
		},
		statsRegistry: stats.NewFinagleStatsRegistry(),
		statsReceiver: statsRec,
		statsCancelFn: cancelFn,
		latchTime: latchTime,
	}
}

// use in a goroutine to print stats every minute
func (st *SchedulingAlgTester) printStats(deps *externalDeps, stopCh chan bool, rm map[sched.Priority] string) {
	ticker := time.NewTicker(deps.latchTime)

	for true {
		select {
		case <-ticker.C:
			st.writeStatsToFile(deps, rm)
		case <-stopCh:
			return
		}
	}
}

func (st *SchedulingAlgTester) writeStatsToFile(deps *externalDeps, rm map[sched.Priority] string) {
	t := time.Now()
	timePP := t.Format(time.RFC3339)
	statsJson := deps.statsReceiver.Render(false)
	var s map[string]interface{}
	json.Unmarshal(statsJson, &s)
	line := make([]byte, 0)
	for priority := 0; priority < len(rm); priority++ {
		req := rm[sched.Priority(priority)]
		runningStatName := fmt.Sprintf("schedNumRunningTasksGauge_%s", req)
		waitingStatName := fmt.Sprintf("schedNumWaitingTasksGauge_%s", req)
		runningCnt := s[runningStatName]
		waitingCnt := s[waitingStatName]
		line = append(line, []byte(fmt.Sprintf("%s, job:%s, priority:%d, running:%v, waiting:%v, ",
			timePP, req, priority, runningCnt, waitingCnt))...)
	}
	line = append(line, '\n')

	f,_ := os.OpenFile(st.statsFileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0777)
	defer f.Close()
	f.Write(line)
	log.Warnf("%s\n",line)
}

func (st *SchedulingAlgTester) makeTestCluster(num int) *testCluster {
	h := &testCluster{
		ch: make(chan []cluster.NodeUpdate, 1),
	}
	nodes := []cluster.Node{}
	for i := 0; i< num; i++ {
		nodes = append(nodes, cluster.NewIdNode(fmt.Sprintf("node%d", i)))
	}
	h.nodes = nodes
	return h
}

func (st *SchedulingAlgTester) getTestConfig() scheduler.SchedulerConfig {
	return scheduler.SchedulerConfig{
		MaxRetriesPerTask:       0,
		DebugMode:               false,
		RecoverJobsOnStartup:    false,
		DefaultTaskTimeout:      0,
		TaskTimeoutOverhead:     0,
		RunnerRetryTimeout:      0,
		RunnerRetryInterval:     0,
		ReadyFnBackoff:          0,
		MaxRequestors:           1000,
		MaxJobsPerRequestor:     1000,
		SoftMaxSchedulableTasks: 0,
		TaskThrottle:            0,
		Admins:                  nil,
	}
}

func (st *SchedulingAlgTester) getRequestorMap(jobDefsMap map[int][]*sched.JobDefinition) map[sched.Priority] string {
	m := make(map[sched.Priority] string)

	var r string
	for _, jobDefs := range jobDefsMap {
		for _, jobDef := range jobDefs {
			r = jobDef.Requestor
			m[jobDef.Priority] = r
		}
	}

	return m
}

func (st *SchedulingAlgTester) printFinalComparison(finishTimes jobFinishTimes, comparisonMap map[string] *timeSummary) {
	fmt.Println("*****************************************************")
	fmt.Println("*****************************************************")
	fmt.Printf("Stats were also written to: %s\n", st.statsFileName)
	fmt.Println("*****************************************************")
	for id, summary := range comparisonMap {
		if finish, ok := finishTimes.times[id]; ok {
			testTime := finish.Sub(summary.testStart)
			delta := summary.prodDuration - testTime
			fmt.Printf("%s, delta,%d, prod,%d, (seconds), test, %d, (seconds)\n",
				summary.buildUrl, int(delta.Seconds()), int(summary.prodDuration.Seconds()), int(testTime.Seconds()))
		} else {
			fmt.Printf("*** job:%s did not have a finish time\n", id)
		}
	}
}