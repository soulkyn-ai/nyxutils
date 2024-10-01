package nyxutils

import (
	"container/heap"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Job func()

type CronJob struct {
	Action   Job
	Interval time.Duration
	RunImmed bool
	nextRun  time.Time
	Name     string
	index    int // The index of the item in the heap.
}

type JobHeap []*CronJob

func (h JobHeap) Len() int { return len(h) }

func (h JobHeap) Less(i, j int) bool {
	return h[i].nextRun.Before(h[j].nextRun)
}

func (h JobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *JobHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*CronJob)
	item.index = n
	*h = append(*h, item)
}

func (h *JobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1 // For safety.
	*h = old[0 : n-1]
	return item
}

type CronManager struct {
	jobHeap  JobHeap
	lock     sync.Mutex
	stopChan chan struct{}
	running  bool
	logger   *zerolog.Logger
	wg       sync.WaitGroup // Added WaitGroup to track dispatcher
}

func NewCronManager(logger *zerolog.Logger) *CronManager {
	return &CronManager{
		jobHeap:  make(JobHeap, 0),
		stopChan: make(chan struct{}),
		logger:   logger,
	}
}

func (m *CronManager) AddCron(name string, job Job, interval time.Duration, runImmed bool) {
	cronJob := &CronJob{
		Action:   job,
		Interval: interval,
		RunImmed: runImmed,
		Name:     name,
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if runImmed {
		cronJob.nextRun = time.Now()
	} else {
		cronJob.nextRun = time.Now().Add(interval)
	}

	heap.Push(&m.jobHeap, cronJob)
	m.logger.Debug().Msgf("[cron|%s|%s] Added as job", name, interval)

	// Signal to the dispatcher that a new job is available.
	if m.running {
		// Close and recreate stopChan to interrupt the dispatcher timer.
		close(m.stopChan)
		m.stopChan = make(chan struct{})
	}
}

func (m *CronManager) runJob(cronJob *CronJob) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error().Msgf("[cron|%s|%s] panicked: %v", cronJob.Name, cronJob.Interval, r)
		}
	}()

	done := make(chan struct{})
	go func() {
		cronJob.Action()
		close(done)
	}()

	select {
	case <-done:
		m.logger.Debug().Msgf("[cron|%s|%s] completed", cronJob.Name, cronJob.Interval)
	case <-time.After(cronJob.Interval):
		m.logger.Error().Msgf("[cron|%s|%s] timed out", cronJob.Name, cronJob.Interval)
	}
}

func (m *CronManager) dispatcher() {
	for {
		m.lock.Lock()
		if len(m.jobHeap) == 0 {
			m.lock.Unlock()
			// Wait for a new job to be added or stop signal.
			select {
			case <-m.stopChan:
				return
			}
		} else {
			// Get the job with the earliest nextRun time.
			nextJob := m.jobHeap[0]
			now := time.Now()
			delay := nextJob.nextRun.Sub(now)
			m.lock.Unlock()

			if delay <= 0 {
				// Time to run the job.
				m.lock.Lock()
				heap.Pop(&m.jobHeap)
				m.lock.Unlock()

				m.runJob(nextJob)

				// Reschedule the job.
				nextJob.nextRun = time.Now().Add(nextJob.Interval)

				m.lock.Lock()
				heap.Push(&m.jobHeap, nextJob)
				m.lock.Unlock()
			} else {
				// Wait until the next job's scheduled time.
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
					// Time to check the jobs again.
				case <-m.stopChan:
					timer.Stop()
					return
				}
			}
		}
	}
}

func (m *CronManager) Start() {
	m.lock.Lock()
	if m.running {
		m.lock.Unlock()
		return
	}
	m.running = true
	m.stopChan = make(chan struct{})
	m.wg.Add(1) // Added to track dispatcher
	m.lock.Unlock()
	go func() {
		m.dispatcher()
		m.wg.Done() // Signal that dispatcher has exited
	}()
}

func (m *CronManager) Stop() {
	m.lock.Lock()
	if !m.running {
		m.lock.Unlock()
		return
	}
	m.running = false
	close(m.stopChan)
	m.lock.Unlock()
	m.wg.Wait() // Wait for dispatcher to finish
}
