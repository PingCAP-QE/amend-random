package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
)

type Log struct {
	sync.RWMutex
	threadCnt   int
	threadNames map[string]int
	threads     []string
	logs        [][]LogOne
}

type LogOne struct {
	sql   string
	err   error
	start time.Time
	end   time.Time
}

func newLog() Log {
	return Log{
		threadCnt:   0,
		threadNames: make(map[string]int),
		threads:     []string{},
		logs:        [][]LogOne{},
	}
}

func (l *Log) NewThread(name string) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := l.threadNames[name]; ok {
		return errors.Errorf("thread %s already exist", name)
	}
	l.threadNames[name] = l.threadCnt
	l.threadCnt++
	l.threads = append(l.threads, name)
	l.logs = append(l.logs, []LogOne{})
	return nil
}

func (l *Log) Exec(name, sql string) int {
	l.Lock()
	defer l.Unlock()
	threadIndex, ok := l.threadNames[name]
	if !ok {
		panic("use uninit thread %s" + name)
	}
	logIndex := len(l.logs[threadIndex])
	l.logs[threadIndex] = append(l.logs[threadIndex], LogOne{
		sql:   sql,
		start: time.Now(),
	})
	if logIndex >= len(l.logs[threadIndex]) {
		fmt.Println(name, threadIndex, l.logs[threadIndex])
	}
	return logIndex
}

func (l *Log) Done(name string, logIndex int, err error) {
	l.Lock()
	defer l.Unlock()
	threadIndex, ok := l.threadNames[name]
	if !ok {
		panic("use uninit thread %s" + name)
	}
	if logIndex >= len(l.logs[threadIndex]) {
		fmt.Println(name, threadIndex, l.logs[threadIndex])
	}
	l.logs[threadIndex][logIndex].end = time.Now()
	l.logs[threadIndex][logIndex].err = err
}

var emptyTime = time.Time{}

const LOGTIME_FORMAT = "2006-01-02 15:04:05.00000"

func (l *Log) Dump(dir string) {
	startTime := time.Now().Format("2006-01-02_15:04:05")
	logPath := path.Join(dir, startTime)
	if err := os.MkdirAll(logPath, 0755); err != nil {
		fmt.Println("error create log dir", err)
		return
	}
	var wg sync.WaitGroup
	l.RLock()
	for threadName, threadIndex := range l.threadNames {
		wg.Add(1)
		go func(threadName string, threadIndex int) {
			var b strings.Builder
			threadLogs := l.logs[threadIndex]
			for _, log := range threadLogs {
				if log.err == nil && log.end != emptyTime {
					fmt.Fprintf(&b, "[SUCCESS]")
				} else {
					if log.err != nil {
						fmt.Fprintf(&b, "[FAILED %s]", log.err.Error())
					} else {
						fmt.Fprintf(&b, "[UNFINISHED]")
					}
				}
				fmt.Fprintf(&b, " [%s-%s] ", log.start.Format(LOGTIME_FORMAT), log.end.Format(LOGTIME_FORMAT))
				b.WriteString(log.sql)
				b.WriteString("\n")
			}
			logFile, err := os.Create(path.Join(logPath, fmt.Sprintf("%s.log", threadName)))
			if err != nil {
				fmt.Printf("create %s log failed\n", threadName)
				wg.Done()
				return
			}
			logWriter := bufio.NewWriter(logFile)
			if _, err := logWriter.WriteString(b.String()); err != nil {
				fmt.Printf("write %s log failed\n", threadName)
				wg.Done()
				return
			}
			if err := logWriter.Flush(); err != nil {
				fmt.Printf("flush %s log failed\n", threadName)
				wg.Done()
				return
			}
			if err := logFile.Close(); err != nil {
				fmt.Printf("close %s log failed\n", threadName)
				wg.Done()
				return
			}
			wg.Done()
		}(threadName, threadIndex)
	}
	wg.Wait()
	l.RUnlock()
}
