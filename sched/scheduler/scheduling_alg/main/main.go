package main

import (
	"flag"
	"fmt"
	"github.com/cenkalti/backoff"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/scheduler/scheduling_alg"
)

const (
	defaultSqJobs = "source_queue_phab_production,source_auto_sandbox_phab_production,source_ci"
	// args
	modeArg           = "mode"
	ciuiArg           = "ciui"
	jobNamesArg       = "jobNames"
	firstBuildArg     = "firstBuild"
	jobPrioritiesArg  = "jobPriorities"
	priorityRatiosArg = "priorities"
	shadowDurationArg = "shadowDuration"
	clusterSizeArg    = "clusterSize"
	logLevelArg       = "logLevel"
	dataCenterArg     = "dataCenter"
)



func main() {

	args := parseArgs()

	level, err := log.ParseLevel(args[logLevelArg].(string))
	if err != nil {
		log.Error(err)
		return
	}
	log.SetReportCaller(true)
	log.SetLevel(level)

	var jobDefs map[int] []*sched.JobDefinition
	var pRatios []int

	if args[modeArg] == "shadow" {
		//defaulSqJobPriorities := [...]int{0, 1, 2, 3}
		//priorities := [...]int{1, 50, 2, 10}
		ls, e := MakeLogScraper(args[ciuiArg].(string),
			strings.Split(args[jobNamesArg].(string), ","),
			strings.Split(args[firstBuildArg].(string), ","),
			time.Duration(args[shadowDurationArg].(int)) * time.Minute,
			args[jobPrioritiesArg].(string),
			args[priorityRatiosArg].(string),
			args[dataCenterArg].(string))

		if e != nil {
			log.Fatalf(e.Error())
			return
		}

		jobDefs, e = ls.BuildJobDefinitions()
		if e != nil {
			log.Fatalf(e.Error())
			return
		}
		pRatios = ls.priorityRatios
		log.Warnf("total tasks:%d",ls.totalTasks)
	} else {
		t := [...]int{1, 5, 3}
		pRatios = t[:]

		jobDefs = makeJobDefs()

	}
	schedAlg := scheduling_alg.MakeSchedulingAlgTester()
	e := schedAlg.RunTest(jobDefs, pRatios[:], args[clusterSizeArg].(int))
	if e != nil {
		fmt.Sprintf("%s\n", e.Error())
	}

}

func parseArgs() map[string]interface{} {
	mode := flag.String("mode", "test", "test/shadow: run the hardcoded tests or shadow real jobs")
	ciuiAddr := flag.String("ciui_addr", "https://ci-ui.twitter.biz", "'ciui address. default: http://go/ciui")
	jobNames := flag.String("job_names",
		defaultSqJobs, fmt.Sprintf("comma delimited list of jobs to process. Default: %s", defaultSqJobs))
	firstBuilds := flag.String("first_builds", "", "comma delimited list of build number for each job")
	priorities := flag.String("priorities", "", "the priority (0, 1, 2, ...) for each job. Defaults to 0, 1, 2, 3, ...")
	priorityRatios := flag.String("priority_ratios", "", "the run ratio for each priority.  Defaults to 1, 10, 10, 10 ... ")
	dataCenter := flag.String("datacenter", "smf1", "only shadow builds from this datacenter")
	shadowDuration := flag.Int("minutes", 20, "number of minutes to shadow.")
	clusterSize := flag.Int("cluster_size", 5, "simulated cluster size, default 5")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	args := make(map[string]interface{})
	args[modeArg] = *mode
	args[ciuiArg] = *ciuiAddr
	args[jobNamesArg] = *jobNames
	args[firstBuildArg] = *firstBuilds
	args[shadowDurationArg] = *shadowDuration
	args[logLevelArg] = *logLevelFlag
	args[priorityRatiosArg] = *priorityRatios
	args[jobPrioritiesArg] = *priorities
	args[clusterSizeArg] = *clusterSize
	args[dataCenterArg] = *dataCenter

	return args
}

func makeJobDefs() map[int] []*sched.JobDefinition {
	jd := make([]sched.JobDefinition, 3)

	jd[0].Basis = fmt.Sprintf("%d", 0 * time.Second) // run right away
	jd[1].Basis = fmt.Sprintf("%d", 10 * time.Second) // run 10 seconds into the test
	jd[2].Basis = fmt.Sprintf("%d", 20 * time.Second) // run 20 seconds into the test

	m := make(map[int] []*sched.JobDefinition)
	for i := 0; i < 3; i++ {
		jd[i].JobType = "dummyJobType"
		jd[i].Requestor = fmt.Sprintf("dummyRequestor%d", i)
		jd[i].Tag = "{url:dummy_job, elapsedMin:3}"
		jd[i].Priority = sched.Priority(i)
		jd[i].Tasks = makeDummyTasks()
		t := int(time.Now().Add(time.Duration(-1 * i) * time.Minute).Unix())
		m[t] = make([]*sched.JobDefinition, 1)
		m[t][0] = &jd[i]
	}

	return m
}

func makeDummyTasks() []sched.TaskDefinition {
	//cnt := int(rand.Float64() * 10)+ 1
	cnt := 6

	tasks := make([]sched.TaskDefinition, cnt)
	for i := 0; i < cnt ; i++ {

			td := runner.Command{
			Argv:           []string{"cmd", "30", "0"},  // dummy cmd, sleep time, exit code
			EnvVars:        nil,
			Timeout:        0,
			SnapshotID:     "",
			LogTags: tags.LogTags{TaskID: fmt.Sprintf("%d", i),Tag:"dummyTag",},
			ExecuteRequest: nil,
		}
		tasks[i] = sched.TaskDefinition{td}
	}

	return tasks
}

type LogScraper struct {
	httpCli    *http.Client
	ciuiAddr   string
	priorities []int
	priorityRatios []int
	dc string // only process jobs run on this data center

	jobNames []string
	startBuildNums []string
	shadowDuration time.Duration

	startTime time.Time
	endTime time.Time

	maxTaskTime int
	totalTasks int

	httpClient http.Client

	// scraper res
	sqStartRe *regexp.Regexp
	sqEndRe *regexp.Regexp
	autoSqEndRe *regexp.Regexp
	logURLRe *regexp.Regexp
	taskCompletionRe *regexp.Regexp
	taskFailureRe *regexp.Regexp
	dcRe *regexp.Regexp
}

func MakeLogScraper(ciuiAddr string, jobNames []string, firstBuilds []string, shadowDuration time.Duration,
	priorities string, priorityRatios string, dc string) (*LogScraper, error) {

	ls :=  &LogScraper{
		ciuiAddr:       ciuiAddr,
		jobNames:       jobNames,
		startBuildNums: firstBuilds,
		shadowDuration: shadowDuration,
		httpCli:        &http.Client{},
		dc:				dc,
		maxTaskTime:    0,
		totalTasks:     0,
		httpClient:     http.Client{Timeout: 7 * time.Minute},
		logURLRe: regexp.MustCompile("https[^\"]*"),
		taskCompletionRe: regexp.MustCompile("Task Success: .+ finished in [<]{0,1}([0-9]+)s"),
		taskFailureRe: regexp.MustCompile("Task Failure: .+ finished in [<]{0,1}([0-9]+)s"),
		sqStartRe: regexp.MustCompile("I] ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])] Starting CloudExec runs for targets"),
		sqEndRe: regexp.MustCompile("I] ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])] [\\*]* Exiting CloudExec"),
		autoSqEndRe: regexp.MustCompile("I] ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])] Rebasing origin/master & checking for KTFd"),
		dcRe: regexp.MustCompile("Using CloudExec running at (\\w+)"),
	}

	var e error
	defRatios := [...]int{1,10,10,10,10,10,10,10,10,10}
	ls.priorityRatios, e = ls.parseIntList(priorityRatios, defRatios[:])
	defPriorities := [...]int{0,1,2,3,4,5,6,7,8,9}
	ls.priorities, e = ls.parseIntList(priorities, defPriorities[:])
	if e != nil {
		return nil, e
	}

	e = ls.setTimeSpan()
	if e != nil {
		return nil, e
	}

	return ls, nil
}

func (ls *LogScraper) setTimeSpan() error {
	// get the minimal timestamp from the set of 'first builds' on each of the jobs
	minTime := time.Now()
	for i, job := range ls.jobNames  {
		url := fmt.Sprintf("%s/log/%s/%s", ls.ciuiAddr, job, ls.startBuildNums[i])
		logUrl, e := ls.httGetRequest(url)
		if e != nil {
			return fmt.Errorf("error issuing get request:%s", e.Error())
		}

		logTime, e := ls.getTimeFromLogUrl(logUrl)
		if e != nil {
			return e
		}
		if logTime.Before(minTime) {
			minTime = logTime
		}
	}

	ls.startTime = minTime
	ls.endTime = ls.startTime.Add(ls.shadowDuration)

	return nil
}

/*
extract the timestamp from the logUrl
logUrl is expected to be the response from https://ci-ui.twitter.biz/log/<jobName>/<num>
.... of the form {"log_url":"<https://.../>YYYY-MM-DD_HH-MM-SS_<buildNum>.html"}
 */
func (ls *LogScraper) getTimeFromLogUrl(logUrl string) (time.Time, error) {
	t := strings.Split(logUrl, "/")
	logTimeStamp := t[len(t)-1]
	// timeStampStr is of the form: YYYY-MM-DD_HH-MM-SS_buildNum.html
	tsParts := strings.Split(logTimeStamp, "_")
	day := tsParts[0]
	buildTime := tsParts[1]
	logTime, err := time.Parse("2006-01-02_15-04-05", fmt.Sprintf("%s_%s", day, buildTime))
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing YYYY-MM-DD_HH-MM-SS time from %s_%s extracted from %s", day, buildTime, logUrl)
	}
	return logTime, nil
}

/*
build a map mapping of jobDefinitions that correspond to builds, of LogScraper's jobNames, that
occurred between LogScraper's start/end time.
Each jobDefinition will contain 1 shadow task for each successful task the corresponding original
Build.  The shadow tasks will contain the elapsed time from the original task

The map structure maps the original job's start time to the shadow job definition
 */
func (ls *LogScraper) BuildJobDefinitions() (map[int] []*sched.JobDefinition, error) {
	jobDefMap := make(map[int] []*sched.JobDefinition)

	for i, jobName := range ls.jobNames  {
		log.Warnf("finding prod jobs in window for %s", jobName)
		buildNum, e := strconv.Atoi(ls.startBuildNums[i])
		if e != nil {
			log.Errorf("Error converting buildnum to int: %s, %s, %s. Skipping job",
				jobName, ls.startBuildNums[i], e.Error())
			continue
		}
		done := false
		for ! done {
			log.Warnf("processing %s/%d", jobName, buildNum)
			startTime, jd, e := ls.buildJobDefinition(jobName, buildNum, ls.priorities[i])
			if e != nil {
				return nil, fmt.Errorf("error building job def for %s/%d. %s",
					jobName, buildNum, e)
			}
			if jd == nil {
				done = true
				continue
			}
			if len(jd.Tasks) == 0 {
				log.Warnf("Skipped job:%s/%d", jobName, buildNum)
				buildNum++
				continue // no tasks in this job, skip it
			}
			unixTime := int(startTime.Unix())
			if _, ok := jobDefMap[unixTime]; ok {
				jobDefMap[unixTime] = append(jobDefMap[unixTime], jd)
			} else {
				t := [1]*sched.JobDefinition {jd}
				jobDefMap[unixTime] = t[:]
			}
			buildNum++
		}
	}

	return jobDefMap, nil
}

/*
build a job definition from the jobname/buildNum
this job definition will contain one task for each successful task in the build's log
the new task's cmd argv[1] will contain the elapsed time (in seconds) that the task ran
 */
func (ls *LogScraper) buildJobDefinition(jobName string, buildNum int, priority int) (time.Time, *sched.JobDefinition,
	error) {
	url := fmt.Sprintf("%s/log/%s/%d", ls.ciuiAddr, jobName, buildNum)

	logUrl, e := ls.httGetRequest(url) // logUrl is { "log_url":"<url>"}
	if e != nil {
		if strings.Contains(e.Error(), "http status: 404 Not Found") {
			// return an empty job definition - there are no more builds in the time window for this job
			return time.Now(), &sched.JobDefinition{
				Tasks:     make([]sched.TaskDefinition, 0),
			}, nil
		}
		return time.Now(), nil, e
	}
	startTime, e := ls.getTimeFromLogUrl(logUrl)
	if e != nil {
		return time.Now(), nil, fmt.Errorf("error extracting time from logUrl:%s, %s", logUrl, e.Error())
	}
	if startTime.After(ls.endTime) {
		log.Warnf("job %s/%d is outside the time window", jobName, buildNum)
		return startTime, nil, nil
	}

	// pull url from {\n\tlogUrl : "<url>"\n}
	url = ls.logURLRe.FindString(logUrl)
	buildLog, e := ls.httGetRequest(url)
	if e != nil {
		return startTime, nil, e
	}
	jd := &sched.JobDefinition{
		JobType:   "dummyType",
		Requestor: jobName,
		Priority:  sched.Priority(priority),
		Tasks: make([]sched.TaskDefinition, 0),
	}

	ok, e := ls.matchesDataCenter(buildLog)
	if e != nil {
		return startTime, nil, fmt.Errorf("couldn't get data center from log %s. %s", url, e.Error())
	}
	if !ok {
		// not targeted datacenter, skip this build
		log.Warnf("%s/%s wrong datacenter", jobName, buildNum)
		return startTime, jd, nil
	}

	// get the task definitions
	taskDefs, e := ls.getTasksFromLog(buildLog)
	if e != nil {
		return startTime, nil, e
	}
	jd.Tasks = taskDefs

	var buildElapsedTime time.Duration
	if len(taskDefs) > 0 {
		buildElapsedTime, e = ls.getElapseTimeFromLog(buildLog)
		if e != nil {
			return startTime, nil, fmt.Errorf("couldn't get build elapsed time from %s. %s", url, e.Error())
		}
	}
	jd.Tag = fmt.Sprintf("{url:%s, elapsedMin:%d}", url, int(buildElapsedTime.Minutes()))

	durationAfterStart := startTime.Sub(ls.startTime)
	jd.Basis = fmt.Sprintf("%d", durationAfterStart)

	return startTime, jd, nil

}

func (ls *LogScraper) matchesDataCenter(buildLog string) (bool, error) {
	m := ls.dcRe.FindStringSubmatch(buildLog)
	if len(m) != 2 {
		return false, nil
	}
	if m[1] == ls.dc {
		return true, nil
	}
	return false, nil
}

func (ls *LogScraper) httGetRequest(url string) (string, error) {
	try := 1
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 5 * time.Minute
	b.Reset()
	var resp *http.Response
	log.Infof("issuing %s", url)
	err := backoff.Retry(func() error {
		var err error
		if try > 1 {
			log.Infof("Try #%d on %s", try, url)
		}
		resp, err = ls.httpClient.Get(url)   // issue the get()
		try += 1
		if err != nil {
			log.Infof("read got %s err", err.Error())
		}
		return err
	}, b)

	if err != nil {
		return "", fmt.Errorf("error getting url: %s, %s", url, err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status not OK for url: %s, http status: %s", url, resp.Status)
	}
	var data []byte
	size := resp.ContentLength
	try = 1
	b.Reset()
	err = backoff.Retry(func() error {
		var err error
		if try > 1 {
			log.Infof("Try #%d on reading %d size Body for %s", try, size, url)
		}
		data, err = ioutil.ReadAll(resp.Body)    // get the body of the response
		try += 1
		if err != nil {
			log.Infof("read got %s err", err.Error())
		}
		return err
	}, 	b)
	if err != nil {
		return "", fmt.Errorf("error reading %s, resp body: %s", url, err.Error())
	}

	log.Infof("returning %d size result", len(data))
	return string(data), nil
}

func (ls *LogScraper) getTasksFromLog(runLog string) ([]sched.TaskDefinition, error) {
	retDefs := make([]sched.TaskDefinition,0)
	tasks := ls.taskCompletionRe.FindAllStringSubmatch(runLog, -1)
	failedTasks := ls.taskFailureRe.FindAllStringSubmatch(runLog, -1)
	tasks = append(tasks, failedTasks[:]...)

	for i, task := range tasks {
		d, e := strconv.Atoi(task[1])
		if e != nil {
			return nil, fmt.Errorf("couldn't parse task duration:%s, %s", task[1], e.Error())
		}
		if d > ls.maxTaskTime {
			ls.maxTaskTime = d
		}
		if d > 1000 {
			log.Warnf("found task duration of %d", d)  //have this line for setting a breakpoint
		}
		td := runner.Command{
			Argv:           []string{"elapsed task time:", task[1], "0"}, // any value, sleep time, exit code
			EnvVars:        nil,
			Timeout:        0,
			SnapshotID:     "",
			LogTags:        tags.LogTags{TaskID: fmt.Sprintf("task%d", i), Tag: "dummyTag",},
			ExecuteRequest: nil,
		}
		retDefs = append(retDefs, sched.TaskDefinition{td})
	}

	ls.totalTasks += len(retDefs)
	return retDefs, nil
}

/*
parse a comma delimited list of integers into an []int
 */
func (ls *LogScraper) parseIntList(valStr string, defaultVals []int) ([]int, error) {
	if strings.Trim(valStr, " ") == "" {
		return defaultVals, nil
	}
	ratiosStr := strings.Split(valStr, ",")
	ratiosArry := make([]int, len(ratiosStr))
	for i, t := range ratiosStr {
		r, e := strconv.Atoi(strings.Trim(t, " "))
		if e != nil {
			return nil, fmt.Errorf("Error parsing: %s, value: %s. %s", ratiosStr, t, e.Error())
		}
		ratiosArry[i] = r
	}
	return ratiosArry, nil
}

/*
get the elapsed time for running the tests in production
 */
func (ls *LogScraper) getElapseTimeFromLog(buildLog string) (time.Duration, error) {
	y, m, d := ls.startTime.Date()

	start := ls.sqStartRe.FindStringSubmatch(buildLog)
	if len(start) != 4 {
		return time.Duration(0), fmt.Errorf("couldn't find Exiting Cloud Exec timestamp")
	}
	startStr := fmt.Sprintf("%d-%02d-%02d %s:%s:%s", y, m, d, start[1], start[2], start[3])
	startTime, e := time.Parse("2006-01-02 15:04:05", startStr)
	if e != nil {
		return time.Duration(0), fmt.Errorf("error parsing start time:%s, %s", startStr, e.Error())
	}

	end := ls.sqEndRe.FindStringSubmatch(buildLog)
	if len(end) != 4 {
		end = ls.autoSqEndRe.FindStringSubmatch(buildLog)
		if len(end) != 4 {
			return time.Duration(0), fmt.Errorf("couldn't find Exiting Cloud Exec timestamp")
		}
	}
	endStr := fmt.Sprintf("%d-%02d-%02d %s:%s:%s", y, m, d, end[1], end[2], end[3])
	endTime, e := time.Parse("2006-01-02 15:04:05", endStr)
	if e != nil {
		return time.Duration(0), fmt.Errorf("error parsing end time:%s, %s", endStr, e.Error())
	}

	if endTime.Before(startTime) {
		endTime = endTime.Add(24 * time.Hour)
	}

	return endTime.Sub(startTime), nil
}

