package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
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

func (l *Log) Dump(dir string) string {
	startTime := time.Now().Format("2006-01-02_15:04:05")
	logPath := path.Join(dir, startTime)
	if err := os.MkdirAll(logPath, 0755); err != nil {
		fmt.Println("error create log dir", err)
		return logPath
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
	return logPath
}

func (l *Log) DumpTimeline(file string) error {
	l.RLock()
	size, maxTime := 0, int64(0)
	for _, entries := range l.logs {
		size += len(entries)
	}
	rows := make([][4]interface{}, 0, size)
	for i, thread := range l.threads {
		for _, entry := range l.logs[i] {
			start := entry.start.UnixNano() / int64(time.Millisecond)
			end := entry.end.UnixNano() / int64(time.Millisecond)
			message := entry.sql
			if entry.err != nil {
				message += " [ERR: " + entry.err.Error() + "]"
			}
			if maxTime < end {
				maxTime = end
			} else if maxTime < start {
				maxTime = start + 1
			}
			rows = append(rows, [4]interface{}{thread, message, start, end})
		}
	}
	l.RUnlock()
	for i := range rows {
		if rows[i][3] == 0 {
			rows[i][3] = maxTime
		}
	}
	out, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(out, `<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['timeline']});
      google.charts.setOnLoadCallback(drawChart);

      async function drawChart() {
        var container = document.getElementById('timeline');
        var chart = new google.visualization.Timeline(container);
        var dataTable = new google.visualization.DataTable();
        var data = `)
	if err != nil {
		return err
	}
	err = json.NewEncoder(out).Encode(rows)
	if err != nil {
		return err
	}
	out.Seek(-1, io.SeekCurrent)
	_, err = fmt.Fprint(out, `;

        dataTable.addColumn({ type: 'string', id: 'Thread' });
        dataTable.addColumn({ type: 'string', id: 'Message'})
        dataTable.addColumn({ type: 'number', id: 'Start' });
        dataTable.addColumn({ type: 'number', id: 'End' });
        dataTable.addRows(data);

        chart.draw(dataTable, {
          timeline: { showBarLabels: false, colorByRowLabel: true },
        });
      }
    </script>
  </head>
  <body>
    <div id="timeline" style="width: 100%; height: 100%;"></div>
  </body>
</html>
`)
	if err != nil {
		return err
	}
	return nil
}
