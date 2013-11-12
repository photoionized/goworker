package goworker

import (
	"github.com/cihub/seelog"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
  logger seelog.LoggerInterface
	queuesString   string
	queues         queuesFlag
	intervalFloat  float64
	interval       intervalFlag
	concurrency    int
	connections    int
	uri            string
	namespace      string
	exitOnComplete bool
	isStrict       bool
)

type Worker struct {
  Queues  string,
  IntervalFloat float64,
  Concurrency int
  Connections int
  Uri string
  Namespace string
  ExitOnComplete  bool
}

// Call this function to run goworker. Check for errors in
// the return value. Work will take over the Go executable
// and will run until a QUIT, INT, or TERM signal is
// received, or until the queues are empty if the
// -exit-on-complete flag is set.
func (w *Worker) Work() error {
	var err error
	logger, err = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.InfoLvl)
	if err != nil {
		return err
	}

  queuesString = w.Queues
  intervalFloat = w.IntervalFloat
  concurrency = w.Concurrency
  uri = w.Uri
  namespace = w.Namespace
  exitOnComplete = w.ExitOnComplete

	if err := queues.Set(queuesString); err != nil {
		return err
	}
	if err := interval.SetFloat(intervalFloat); err != nil {
		return err
	}
	isStrict = strings.IndexRune(queuesString, '=') == -1

	quit := signals()

	pool := newRedisPool(uri, connections, connections, time.Minute)
	defer pool.Close()

	poller, err := newPoller(queues, isStrict)
	if err != nil {
		return err
	}
	jobs := poller.poll(pool, time.Duration(interval), quit)

	var monitor sync.WaitGroup

	for id := 0; id < concurrency; id++ {
		worker, err := newWorker(strconv.Itoa(id), queues)
		if err != nil {
			return err
		}
		worker.work(pool, jobs, &monitor)
	}

	monitor.Wait()

	return nil
}
