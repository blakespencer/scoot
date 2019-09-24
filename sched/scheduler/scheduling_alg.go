package scheduler

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/sched"
	"math"
)

type OrigSchedulingAlg struct {}

func (oa *OrigSchedulingAlg) GetTasksToBeAssigned(jobs []*jobState, stat stats.StatsReceiver, cs *clusterState, requestors map[string][]*jobState, cfg SchedulerConfig) []*taskState {
	// Sort jobs by priority and count running tasks.
	// An array indexed by priority. The value is the subset of jobs in fifo order for the given priority.
	priorityJobs := [][]*jobState{[]*jobState{}, []*jobState{}, []*jobState{}}
	for _, job := range jobs {
		p := int(job.Job.Def.Priority)
		priorityJobs[p] = append(priorityJobs[p], job)
	}
	stat.Gauge(stats.SchedPriority0JobsGauge).Update(int64(len(priorityJobs[sched.P0])))
	stat.Gauge(stats.SchedPriority1JobsGauge).Update(int64(len(priorityJobs[sched.P1])))
	stat.Gauge(stats.SchedPriority2JobsGauge).Update(int64(len(priorityJobs[sched.P2])))
	// Assign each job the minimum number of nodes until free nodes are exhausted.
	// Priority2 jobs consume all remaining idle nodes up to a limit, and are preferred over Priority1 jobs.
	// Priority1 jobs consume all remaining idle nodes up to a limit, and are preferred over Priority0 jobs.
	// Priority0 jobs consume all remaining idle nodes up to a limit.
	//
	var tasks []*taskState
	// The number of healthy nodes we can assign
	numFree := cs.numFree()
	// A map[requestor]map[tag]bool{} that makes sure we process all tags for a given requestor once as a batch.
	tagsSeen := map[string]map[string]bool{}
	// An array indexed by priority. The value is a list of jobs, each with a list of tasks yet to be scheduled.
	// 'Optional' means tasks are associated with jobs that are already running with some minimum node quota.
	// 'Required' means tasks are associated with jobs that haven't yet reach a minimum node quota.
	// This distinction makes sure we don't starve lower priority jobs in the second-pass 'remaining' loop.
	remainingOptional := [][][]*taskState{[][]*taskState{}, [][]*taskState{}, [][]*taskState{}}
	remainingRequired := [][][]*taskState{[][]*taskState{}, [][]*taskState{}, [][]*taskState{}}
Loop:
	for _, p := range []sched.Priority{sched.P2, sched.P1, sched.P0} {
		for _, job := range priorityJobs[p] {
			// The number of available nodes for this priority is the remaining free nodes
			numAvailNodes := numFree
			if numAvailNodes == 0 {
				break Loop
			}

			// If we've seen this tag for this requestor before then it's already been handled, so skip this job.
			def := &job.Job.Def
			if tags, ok := tagsSeen[def.Requestor]; ok {
				if _, ok := tags[def.Tag]; ok {
					continue
				}
			} else {
				tagsSeen[def.Requestor] = map[string]bool{}
			}
			tagsSeen[def.Requestor][def.Tag] = true

			// Find all jobs with the same requestor/tag combination and add their unscheduled tasks to 'unsched'.
			// Also keep track of how many total tasks were requested for these jobs and how many are currently running.
			// If later jobs in this group have a higher priority, we handle it by scheduling those first within the group.
			numTasks := 0
			numRunning := 0
			numCompleted := 0
			unsched := []*taskState{}
			for _, j := range requestors[def.Requestor] {
				if j.Job.Def.Tag == def.Tag {
					numTasks += len(j.Tasks)
					numCompleted += j.TasksCompleted
					numRunning += j.TasksRunning
					// Prepend tasks to handle the likely desire for immediate retries for failed tasks in a previous job.
					// Hardcoded for now as this is the way scheduler is currently invoked by customers.
					unsched = append(j.getUnScheduledTasks(), unsched...)
					// Stop checking for unscheduled tasks if they exceed available nodes (we'll cap it below).
					if len(unsched) >= numAvailNodes {
						break
					}
				}
			}
			// No unscheduled tasks, continue onto the next job.
			if len(unsched) == 0 {
				continue
			}

			// How many of the requested tasks can we assign based on the max healthy task load for our cluster.
			// (the schedulable count is the minimum number of nodes appropriate for the current set of tasks).
			numScaledTasks := ceil(float32(numTasks) * cfg.GetNodeScaleFactor(len(cs.nodes), p))
			numSchedulable := 0
			numDesiredUnmet := 0

			// For this group of tasks we want the lesser of: the number remaining, or a number based on load.
			numDesired := min(len(unsched), numScaledTasks)
			// The number of tasks we can schedule is reduced by the number of tasks we're already running.
			// Further the number we'd like and the number we'll actually schedule are restricted by numAvailNodes.
			// If numDesiredUnmet==0 then any remaining outstanding tasks are low priority and appended to remainingOptional
			numDesiredUnmet = max(0, numDesired-numRunning-numAvailNodes)
			numSchedulable = min(numAvailNodes, max(0, numDesired-numRunning))

			if numSchedulable > 0 {
				log.WithFields(
					log.Fields{
						"jobID":          job.Job.Id,
						"priority":       p,
						"numTasks":       numTasks,
						"numSchedulable": numSchedulable,
						"numRunning":     numRunning,
						"numCompleted":   numCompleted,
						"tag":            job.Job.Def.Tag,
					}).Info("Schedulable tasks")
				log.WithFields(
					log.Fields{
						"jobID":           job.Job.Id,
						"unsched":         len(unsched),
						"numAvailNodes":   numAvailNodes,
						"numScaledTasks":  numScaledTasks,
						"numDesiredUnmet": numDesiredUnmet,
						"numRunning":      numRunning,
						"tag":             job.Job.Def.Tag,
					}).Debug("Schedulable tasks dbg")
				tasks = append(tasks, unsched[0:numSchedulable]...)
				// Get the number of nodes we can take from the free node pool
				numFromFree := min(numFree, numSchedulable)
				// Deduct from the number of free nodes - this value gets used after we exit this loop.
				numFree -= numFromFree
			}

			// To both required and optional, append an array containing unscheduled tasks for this requestor/tag combination.
			if numDesiredUnmet > 0 {
				remainingRequired[p] = append(remainingRequired[p], unsched[numSchedulable:numSchedulable+numDesiredUnmet])
			}
			if numSchedulable+numDesiredUnmet < len(unsched) {
				remainingOptional[p] = append(remainingOptional[p], unsched[numSchedulable+numDesiredUnmet:])
			}
		}
	}
	// Distribute a minimum of 75% free to priority=2, 20% to priority=1 and 5% to priority=0
	numFreeBasis := numFree
LoopRemaining:
	// First, loop using the above percentages, and next, distribute remaining free nodes to the highest priority tasks.
	// Do the above loops twice, once to exhaust 'required' tasks and again to exhaust 'optional' tasks.
	for j, quota := range [][]float32{NodeScaleAdjustment, []float32{1, 1, 1}, NodeScaleAdjustment, []float32{1, 1, 1}} {
		remaining := &remainingRequired
		if j > 1 {
			remaining = &remainingOptional
		}
		for _, p := range []sched.Priority{sched.P2, sched.P1, sched.P0} {
			if numFree == 0 {
				break LoopRemaining
			}
			// The remaining tasks, bucketed by job, for a given priority.
			taskLists := &(*remaining)[p]
			// Distribute the allowed number of free nodes evenly across the bucketed jobs for a given priority.
			nodeQuota := ceil((float32(numFreeBasis) * quota[p]) / float32(len(*taskLists)))
			for i := 0; i < len(*taskLists); i++ {
				taskList := &(*taskLists)[i]
				// Noting that we use ceil() above, we may use less quota than assigned if it's unavailable or unneeded.
				nTasks := min(numFree, nodeQuota, len(*taskList))
				if nTasks > 0 {
					// Move the given number of tasks from remaining to the list of tasks that will be assigned nodes.
					log.WithFields(
						log.Fields{
							"nTasks":   nTasks,
							"jobID":    (*taskList)[0].JobId,
							"priority": p,
							"numFree":  numFree,
							"tag":      (*taskList)[0].Def.Tag,
						}).Info("Assigning additional free nodes for each remaining task in job")
					numFree -= nTasks
					tasks = append(tasks, (*taskList)[:nTasks]...)
					// Remove jobs that have run out of runnable tasks.
					if len(*taskList)-nTasks > 0 {
						*taskList = (*taskList)[nTasks:]
					} else {
						*taskLists = append((*taskLists)[:i], (*taskLists)[i+1:]...)
						i--
					}
				}
			}
		}
	}
	return tasks
}


type PriorityBasedAlg struct {
	PriorityRatios      []int  // array of priority ratios where array index == priority
	taskModCounter      float64
	jobsByPriority	    [][]*jobState  // list of jobs at each priority
	runRatios           []float64 // when to run each priority
	roundRobinJobIndex  []int     // for each priority, track the index of the job whose task should be assigned next
	numBarredFromLowPriority int  // number of nodes reserved for non-lowest priority tasks
}

func MakePriorityBasedAlg(ratios []int) *PriorityBasedAlg {
	pbs := &PriorityBasedAlg{
		PriorityRatios:      ratios,
		taskModCounter:      1,
	}

	// initialize the runRatios array
	// runRatio[pN] represents that we want to run tasks for pN 1 out of runRatio[pN] tasks
	// when taskModCounter == runRatio[pN] a task from a job with that priority will be scheduled
	pbs.runRatios = make([]float64, len(pbs.PriorityRatios))
	pbs.runRatios[0] = 1
	pbs.roundRobinJobIndex = make([]int, len(pbs.PriorityRatios))
	for i := 1; i < len(pbs.PriorityRatios); i++ {
		pbs.runRatios[i] = pbs.runRatios[i-1] * float64(pbs.PriorityRatios[i])
	}

	return pbs
}

/*
This is the entry point to the scheduling algorithm
It returns the list of tasks that should be assigned to nodes as per the
algorithm documented at
https://docs.google.com/document/d/1MqN2oAKRHHi_k29fYyUdYfw7oDN8sii0B3yB1_Yqk34/edit
TODO - fix the prior link if ported to confluence
 */
func (pbs *PriorityBasedAlg) GetTasksToBeAssigned(jobs []*jobState, statNotUsed stats.StatsReceiver, cs *clusterState, jobsByRequestorNotUsed map[string][]*jobState, cfgNotUsed SchedulerConfig) []*taskState {
	idleNodeCnt := cs.numFree()
	pbs.numBarredFromLowPriority = 300  // TODO make this 10% of num nodes
	tasksToAssign := make([] *taskState, 0)
	selectedTasks := make(map[string] *taskState)

	// build the list of jobs at each priority
	for i:=0; i < len(pbs.PriorityRatios); i++ {  // initialize priority to jobs array
		pbs.jobsByPriority = make([][]*jobState, len(pbs.PriorityRatios))
	}
	for _, job := range(jobs) {
		if job.TasksCompleted + job.TasksRunning < len(job.Tasks) {
			// the job still has tasks to assign
			priority := int(job.Job.Def.Priority)
			if pbs.jobsByPriority[priority] == nil {
				pbs.jobsByPriority[priority] = make([]*jobState, 0)
			}
			pbs.jobsByPriority[priority] = append(pbs.jobsByPriority[priority], job )
		}
	}

	// reset the priority based job round robin index
	for i := 0; i < len(pbs.PriorityRatios) ; i++ {
		pbs.roundRobinJobIndex[i] = 0
	}

	for i := 0; i < idleNodeCnt; i++ {
		p := pbs.getPriorityToAssign(i, idleNodeCnt)
		task := pbs.getNextTask(p, pbs.jobsByPriority, selectedTasks, i, idleNodeCnt)
		if task != nil {
			tasksToAssign = append(tasksToAssign, task)
		} else if i > pbs.numBarredFromLowPriority {
			// task will = nil if only lowest priority tasks are waiting and we
			// are within the number of nodes reserved for non-lowest priority.
			// so only break when we didn't get a task and we are outside the
			// idle nodes reserved for non-lowest priority tasks
			break
		}
	}

	taskKeys := make([]string, 0, len(tasksToAssign))
	for _, task := range(tasksToAssign) {
		taskKeys = append(taskKeys, fmt.Sprintf("%s_%s", task.JobId, task.TaskId))
	}
	//log.Infof("assigning tasks:%v", taskKeys)
	return tasksToAssign
}

// We are looking for the 'next' unscheduled task at the given priority.
// The next task at a given priority level uses a round robin approach to get one
// task from each of the jobs at that priority level.
// If there are no unscheduled tasks at a given priority level, look for a task at the next
// higher priority level.
// If there are no higher level priority tasks, then start looking for a lower level priority task
// params:
// priority : we are trying to fill a task at this priority level
// jobs: the list of jobs at this priority level
// currentSelectedTasks: tasks already selected for scheduling
func (pbs *PriorityBasedAlg) getNextTask(priority int, jobsByPriority [][]*jobState,
	currentSelectedTasks map[string]*taskState, idleNodeIdx, totalIdleNodes int) *taskState {
	// find the next job at priority whose turn it is to have a task scheduled
	origPriority := priority

	// find an unscheduled task at priority
	for priority >= 0 {
		task := pbs.getTaskFromRRJobs(priority, currentSelectedTasks)

		if task != nil {
			return task
		}
		// there are no more jobs at this priority level with a waiting task
		// go to the 'next' priority level
		priority = pbs.getNextPriority(priority, origPriority, idleNodeIdx, totalIdleNodes)
	}
	return nil
}

func (pbs *PriorityBasedAlg) getTaskFromRRJobs(priority int, currentSelectedTasks map[string]*taskState) *taskState {

	if len(pbs.jobsByPriority[priority]) == 0 {
		return nil
	}

	rrIdx := pbs.roundRobinJobIndex[priority]
	if rrIdx >= len(pbs.jobsByPriority[priority]) {
		log.Fatalf("debugging breakpoint line, rrIdx is too big")
	}

	origRRIdx := rrIdx
	for true {
		task := pbs.getUnscheduledTaskInJob(pbs.jobsByPriority[priority][rrIdx], currentSelectedTasks)
		// update the rr index to next job (for next call)
		rrIdx++
		if rrIdx == len(pbs.jobsByPriority[priority]) {
			rrIdx = 0
		}
		if rrIdx == origRRIdx {
			return task
		}
		pbs.roundRobinJobIndex[priority] = rrIdx
		if rrIdx >= len(pbs.jobsByPriority[priority]) {
			log.Fatalf("debugging breakpoint line, rrIdx is too big")
		}

		if task != nil {
			return task
		}
	}
	return nil
}

// get a task that has not started, add it to currentSelectedTasks so it won't be
// selected again in this iteration of GetTasksToBeAssigned()
func(pbs *PriorityBasedAlg) getUnscheduledTaskInJob(job *jobState,
	currentSelectedTasks map[string]*taskState) *taskState {

	// get the next not started task in the job
	for taskId, task := range job.NotStarted {
		key := fmt.Sprintf("%stask%s", job.Job.Id, taskId)
		if _, ok := currentSelectedTasks[key]; ok {
			continue  // the task is already selected
		}
		currentSelectedTasks[key]=task
		return task
	}
	return nil
}

// get the next priority level to assign... work from origPriority down to 0, then
// from origPriority to last priority value
func (pbs *PriorityBasedAlg) getNextPriority(currentPriority, origPriority,
	idleNodeIdx, totalIdleNodes int) int {
	var nextPriority int
	if currentPriority < origPriority {
		nextPriority = currentPriority - 1 // get next higher priority
	} else {
		nextPriority = currentPriority + 1 // get next lower priority
	}
	if nextPriority < 0 {
		nextPriority = origPriority + 1
	}

	withinReservedNodes := (totalIdleNodes - idleNodeIdx) < pbs.numBarredFromLowPriority
	if withinReservedNodes && nextPriority == len(pbs.PriorityRatios) - 1 {
		return -1
	}

	if nextPriority < len(pbs.PriorityRatios) {
			return nextPriority
		}

	return -1
}

// track the index for the next job at the given priority level whose task should be selected
// use a round robin approach
func (pbs *PriorityBasedAlg) roundRobinJobIndexUpdate(priority int, jobs []*jobState) int {
	idx := pbs.roundRobinJobIndex[priority]
	idx++
	if idx == len(jobs) {
		if len(jobs) == 0 {
			idx = -1
		}
		idx = 0
	}
	pbs.roundRobinJobIndex[priority] = idx
	return pbs.roundRobinJobIndex[priority]
}

// find the lowest priority whose runRatio is a factor of the taskCounter
func (pbs *PriorityBasedAlg) getPriorityToAssign(idleNodeIdx, totalIdleNodes int) int {
	nextPriority := -1

	withinReservedNodes := (totalIdleNodes - idleNodeIdx) < pbs.numBarredFromLowPriority
	if pbs.taskModCounter == 1 {
		nextPriority = 0
	} else {
		for i := len(pbs.runRatios) - 1; i >= 0 && nextPriority == -1; i-- {
			if math.Mod(pbs.taskModCounter, pbs.runRatios[i]) == 0 &&
				! (withinReservedNodes && i == len(pbs.runRatios) - 1) {
				nextPriority = i
			}
		}
	}
	pbs.taskModCounter++
	if pbs.taskModCounter > pbs.runRatios[len(pbs.runRatios) - 1] {
		pbs.taskModCounter = 1
	}
	return nextPriority
}
