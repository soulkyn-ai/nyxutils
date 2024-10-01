package nyxutils

import (
	"os"
	"runtime"
	"time"
)

// Monitor provides methods to monitor application metrics.
type Monitor struct {
	interval      time.Duration
	stopChan      chan struct{}
	stoppedChan   chan struct{}
	onMetricsFunc func(Metrics)
}

// Metrics holds the monitoring data.
type Metrics struct {
	OpenFileDescriptors int
	NumGoroutines       int
	Timestamp           time.Time
}

// NewMonitor creates a new Monitor with the specified interval.
// The onMetrics function is called with the latest metrics at each interval.
func NewMonitor(interval time.Duration, onMetrics func(Metrics)) *Monitor {
	return &Monitor{
		interval:      interval,
		stopChan:      make(chan struct{}),
		stoppedChan:   make(chan struct{}),
		onMetricsFunc: onMetrics,
	}
}

// Start begins the monitoring process.
func (m *Monitor) Start() {
	go func() {
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		defer close(m.stoppedChan)

		for {
			select {
			case <-ticker.C:
				metrics := m.collectMetrics()
				if m.onMetricsFunc != nil {
					m.onMetricsFunc(metrics)
				}
			case <-m.stopChan:
				return
			}
		}
	}()
}

// Stop signals the monitoring goroutine to stop.
func (m *Monitor) Stop() {
	close(m.stopChan)
	<-m.stoppedChan // Wait for the goroutine to finish
}

// collectMetrics collects the current metrics.
func (m *Monitor) collectMetrics() Metrics {
	numFDs := countOpenFileDescriptors()
	numGoroutines := runtime.NumGoroutine()
	return Metrics{
		OpenFileDescriptors: numFDs,
		NumGoroutines:       numGoroutines,
		Timestamp:           time.Now(),
	}
}

// countOpenFileDescriptors counts the number of open file descriptors.
// It ensures that any opened directories are properly closed after reading.
func countOpenFileDescriptors() int {
	dir, err := os.Open("/proc/self/fd")
	if err != nil {
		return -1 // Return -1 to indicate an error
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return -1
	}
	return len(names)
}
