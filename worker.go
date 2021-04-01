package goworker

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type worker struct {
	process
}

func newWorker(id string, queues []string) (*worker, error) {
	process, err := newProcess(id, queues)
	if err != nil {
		return nil, err
	}
	return &worker{
		process: *process,
	}, nil
}

func (w *worker) MarshalJSON() ([]byte, error) {
	return json.Marshal(w.String())
}

func (w *worker) start(conn *RedisConn, job *Job) error {
	work := &work{
		Queue:   job.Queue,
		RunAt:   time.Now(),
		Payload: job.Payload,
	}

	buffer, err := json.Marshal(work)
	if err != nil {
		return err
	}

	conn.Send("SET", fmt.Sprintf("%sworker:%s", workerSettings.Namespace, w), buffer)
	logger.Debugf("Processing %s since %s [%v]", work.Queue, work.RunAt, work.Payload.Class)

	return w.process.start(conn)
}

func (w *worker) fail(conn *RedisConn, job *Job, err error) error {
	failure := &failure{
		FailedAt:  time.Now(),
		Payload:   job.Payload,
		Exception: "Error",
		Error:     err.Error(),
		Worker:    w,
		Queue:     job.Queue,
	}
	buffer, err := json.Marshal(failure)
	if err != nil {
		return err
	}
	conn.Send("RPUSH", fmt.Sprintf("%sfailed", workerSettings.Namespace), buffer)

	return w.process.fail(conn)
}

func (w *worker) succeed(conn *RedisConn, job *Job) error {
	conn.Send("INCR", fmt.Sprintf("%sstat:processed", workerSettings.Namespace))
	conn.Send("INCR", fmt.Sprintf("%sstat:processed:%s", workerSettings.Namespace, w))

	return nil
}

func (w *worker) finish(conn *RedisConn, job *Job, err error) error {
	if err != nil {
		w.fail(conn, job, err)
	} else {
		w.succeed(conn, job)
	}
	return w.process.finish(conn)
}

func (w *worker) work(jobs <-chan *Job, monitor *sync.WaitGroup) {
	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection in worker %v: %v", w, err)
		return
	} else {
		w.open(conn)
		PutConn(conn)
	}

	monitor.Add(1)

	go func() {
		defer func() {
			defer monitor.Done()

			conn, err := GetConn()
			if err != nil {
				logger.Criticalf("Error on getting connection in worker %v: %v", w, err)
				return
			} else {
				w.close(conn)
				PutConn(conn)
			}
		}()
		for job := range jobs {
			//Need to restructure to handle reading down into the payload. In older
			//Versions of Ruby, the Class is not expressed up front (Which is a sidekiq convention)

			//Payload = {github.com/shairozan/goworker.Payload}
			// Class = {string} "ActiveJob::QueueAdapters::ResqueAdapter::JobWrapper"
			// Args = {[]interface {}} len:1, cap:4
			//  0 = {interface {} | map[string]interface {}}
			//   0 = job_class -> CreateStack
			//   1 = job_id -> 20a0cbe7-42d3-43f1-808b-35787aa20263
			//   2 = queue_name -> default
			//   3 = arguments -> len:1, cap:1
			//   4 = locale -> en

			if len(job.Payload.Args) == 0 {
				logger.Critical("unable to extract arguments from data collected from redis")
				return
			}

			//Let's get the arguments list
			args := job.Payload.Args[0]
			var jobArguments []string
			var jobClass string

			if argsMap, ok := args.(map[string]interface{}); ok {
				jc := argsMap["job_class"]
				if jcs, ok := jc.(string); ok {
					jobClass = jcs
				}
				arguments := argsMap["arguments"]

				//Set the payload for the job as the contents of the sub arguments
				if argumentsMap, ok := arguments.([]interface{}); ok {
					job.Payload.Args = argumentsMap
				}
			}




			if workerFunc, ok := workers[jobClass]; ok {
				w.run(job, workerFunc)

				logger.Debugf("done: (Job{%s} | %s | %v)", job.Queue, job.Payload.Class, job.Payload.Args)
			} else {
				errorLog := fmt.Sprintf("No worker for %s in queue %s with args %v", jobClass, job.Queue, jobArguments)
				logger.Critical(errorLog)

				conn, err := GetConn()
				if err != nil {
					logger.Criticalf("Error on getting connection in worker %v: %v", w, err)
					return
				} else {
					w.finish(conn, job, errors.New(errorLog))
					PutConn(conn)
				}
			}
		}
	}()
}

func (w *worker) run(job *Job, workerFunc workerFunc) {
	var err error
	defer func() {
		conn, errCon := GetConn()
		if errCon != nil {
			logger.Criticalf("Error on getting connection in worker on finish %v: %v", w, errCon)
			return
		} else {
			w.finish(conn, job, err)
			PutConn(conn)
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint(r))
		}
	}()

	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection in worker on start %v: %v", w, err)
		return
	} else {
		w.start(conn, job)
		PutConn(conn)
	}
	err = workerFunc(job.Queue, job.Payload.Args...)
}
