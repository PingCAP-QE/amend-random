package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/you06/go-mikadzuki/kv"
	"github.com/you06/go-mikadzuki/util"
)

var (
	columnLeast = 2
	columnMost  = 200
	colCnt      = 0
	ddlCnt      = 10
	dmlCnt      = 10
	dmlThread   = 5
	dsn1        = ""
	dsn2        = ""
	tableName   = "t"

	ddlMutex sync.Mutex
	indexSet = make(map[string]struct{})
)

func init() {
	flag.StringVar(&dsn1, "dsn1", "root:@tcp(127.0.0.1:4000)/test?tidb_enable_amend_pessimistic_txn=1", "upstream dsn")
	flag.StringVar(&dsn2, "dsn2", "", "downstream dsn")
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
}

func main() {
	db1, err := sql.Open("mysql", dsn1)
	if err != nil {
		panic(err)
	}
	var db2 *sql.DB
	if dsn2 != "" {
		db2, err = sql.Open("mysql", dsn2)
		if err != nil {
			panic(err)
		}
	}

	round := 0
	for {
		round++
		fmt.Println("round:", round)
		log := newLog()
		if err := once(db1, db2, &log); err != nil {
			fmt.Println(err)
			log.Dump("./log")
			break
		}
	}
}

type ColumnType struct {
	name string
	tp   kv.DataType
	len  int
	null bool
}

func NewColumnType(name string, tp kv.DataType, len int, null bool) ColumnType {
	return ColumnType{
		name: name,
		tp:   tp,
		len:  len,
		null: null,
	}
}

func rdColumns() []ColumnType {
	colCnt = util.RdRange(columnLeast, columnMost)
	columns := make([]ColumnType, colCnt)

	for i := 0; i < colCnt; i++ {
		tp := kv.RdType()
		columns[i] = NewColumnType(fmt.Sprintf("col_%d", i), tp, tp.Size(), util.RdBool())
	}

	return columns
}

func MustExec(db *sql.DB, sql string) {
	if _, err := db.Exec(sql); err != nil {
		panic(err)
	}
}

func createTable(columns, primary []ColumnType) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s(\n", tableName)
	for i, column := range columns {
		if column.len > 0 {
			fmt.Fprintf(&b, "%s %s(%d)", column.name, column.tp, column.len)
		} else {
			fmt.Fprintf(&b, "%s %s", column.name, column.tp)
		}
		if !column.null {
			b.WriteString(" NOT")
		}
		b.WriteString(" NULL")
		if i != len(columns)-1 {
			b.WriteString(",\n")
		}
	}

	var indexes []string
	ps := len(primary)
	if ps > 0 {
		columns := make([]string, ps)
		for i := 0; i < ps; i++ {
			columns[i] = primary[i].name
		}
		indexes = append(indexes, fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(columns, ", ")))
	}

	for _, index := range indexes {
		fmt.Fprintf(&b, ",\n%s", index)
	}

	b.WriteString(")")
	return b.String()
}

func insertSQL(columns []ColumnType, count int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s VALUES", tableName)
	for i := 0; i < count; i++ {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j, column := range columns {
			if j != 0 {
				b.WriteString(", ")
			}
			b.WriteString(column.tp.ValToString(column.tp.RandValue()))
		}
		b.WriteString(")")
	}
	return b.String()
}

func updateBatchSQL(columns []ColumnType) string {
	var b strings.Builder

	fmt.Fprintf(&b, "UPDATE %s SET ", tableName)
	for i := 0; i < util.RdRange(1, 5); i++ {
		if i != 0 {
			b.WriteString(", ")
		}

		l := columns[util.RdRange(0, len(columns))]

		switch l.tp {
		case kv.Int, kv.BigInt, kv.TinyInt:
			fmt.Fprintf(&b, "%s=%s/2", l.name, l.name)
		case kv.Date, kv.Datetime, kv.Timestamp:
			fmt.Fprintf(&b, "%s = ADDDATE(%s, INTERVAL 1 DAY)", l.name, l.name)
		case kv.Char, kv.Varchar, kv.Text:
			fmt.Fprintf(&b, "%s = CONCAT(%s, \"-p\")", l.name, l.name)
		}
	}

	b.WriteString(" WHERE ")
	for i := 0; i < util.RdRange(1, 5); i++ {
		if i != 0 {
			b.WriteString(" AND ")
		}

		col := columns[util.RdRange(0, len(columns))]

		fmt.Fprintf(&b, "%s<%s", col.name, col.tp.ValToString(col.tp.RandValue()))
	}

	return b.String()
}

func addIndex(columns []ColumnType, i int) (string, string) {
	var b strings.Builder
	indexes := make(map[int]struct{})
	indexName := fmt.Sprintf("k%d", i)

	fmt.Fprintf(&b, "CREATE INDEX %s on %s(", indexName, tableName)
	for i := 0; i < util.RdRange(1, 5); i++ {
		index := util.RdRange(0, len(columns))
		if _, ok := indexes[index]; ok {
			continue
		}
		indexes[index] = struct{}{}
		if i != 0 {
			b.WriteString(", ")
		}
		col := columns[index]
		fmt.Fprintf(&b, "`%s`", col.name)
	}
	b.WriteString(")")

	return indexName, b.String()
}

func dropIndex() (string, string) {
	var (
		indexName string
		b         strings.Builder
	)

	ddlMutex.Lock()
	for len(indexSet) == 0 {
		ddlMutex.Unlock()
		time.Sleep(100 * time.Millisecond)
		ddlMutex.Lock()
	}
	l := util.RdRange(0, len(indexSet))
	for index := range indexSet {
		if l == 0 {
			indexName = index
			break
		}
		l--
	}
	delete(indexSet, indexName)
	ddlMutex.Unlock()

	fmt.Fprintf(&b, "ALTER TABLE %s DROP INDEX %s", tableName, indexName)

	return indexName, b.String()
}

func once(db, db2 *sql.DB, log *Log) error {
	indexSet = make(map[string]struct{})
	columns := rdColumns()
	columns[0].null = false
	primary := []ColumnType{columns[0]}
	createTableStmt := createTable(columns, primary)

	MustExec(db, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	MustExec(db, createTableStmt)

	var (
		wg            sync.WaitGroup
		readyDDLWg    sync.WaitGroup
		readyCommitWg sync.WaitGroup
	)
	wg.Add(dmlThread)
	readyDDLWg.Add(dmlThread)
	readyCommitWg.Add(2)

	// ddl threads
	go func() {
		threadName := "create-index"
		util.AssertNil(log.NewThread(threadName))
		readyDDLWg.Wait()
		for i := 0; i < ddlCnt; i++ {
			index, stmt := addIndex(columns, i)
			// fmt.Println(stmt)
			logIndex := log.Exec(threadName, stmt)
			if _, err := db.Exec(stmt); err != nil {
				log.Done(threadName, logIndex, err)
				fmt.Println(err)
			} else {
				log.Done(threadName, logIndex, nil)
				ddlMutex.Lock()
				indexSet[index] = struct{}{}
				ddlMutex.Unlock()
			}
		}

		readyCommitWg.Done()
	}()
	go func() {
		threadName := "drop-index"
		util.AssertNil(log.NewThread(threadName))
		readyDDLWg.Wait()
		for i := 0; i < ddlCnt/2; i++ {
			_, stmt := dropIndex()
			// fmt.Println(stmt)
			logIndex := log.Exec(threadName, stmt)
			if _, err := db.Exec(stmt); err != nil {
				log.Done(threadName, logIndex, err)
				fmt.Println(err)
			} else {
				log.Done(threadName, logIndex, nil)
			}
		}
		readyCommitWg.Done()
	}()

	// dml threads
	for i := 0; i < dmlThread; i++ {
		go func(i int) {
			threadName := fmt.Sprintf("dml-%d", i)
			util.AssertNil(log.NewThread(threadName))
			logIndex := log.Exec(threadName, "BEGIN")
			txn, err := db.Begin()
			util.AssertNil(err)
			log.Done(threadName, logIndex, nil)
			readyDDLWg.Done()
			insertStmt := insertSQL(columns, 10)
			logIndex = log.Exec(threadName, insertStmt)
			_, err = txn.Exec(insertStmt)
			if err != nil {
				log.Done(threadName, logIndex, err)
				fmt.Println(err)
			} else {
				log.Done(threadName, logIndex, nil)
			}
			for i := 0; i < dmlCnt; i++ {
				stmt := updateBatchSQL(columns)
				logIndex := log.Exec(threadName, stmt)
				_, err := txn.Exec(stmt)
				if err != nil {
					log.Done(threadName, logIndex, err)
					fmt.Println(err)
				} else {
					log.Done(threadName, logIndex, nil)
				}
			}
			readyCommitWg.Wait()
			logIndex = log.Exec(threadName, "COMMIT")
			if err := txn.Commit(); err != nil {
				log.Done(threadName, logIndex, err)
			} else {
				log.Done(threadName, logIndex, nil)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	if db2 != nil {
		fmt.Println("wait for sync")
		time.Sleep(10 * time.Second)
		fmt.Println("ready to check")
	}
	return check(db, db2)
}

type QueryItem struct {
	Null      bool
	ValType   *sql.ColumnType
	ValString string
}

func (q *QueryItem) Same(p *QueryItem) bool {
	if q.Null != p.Null {
		return false
	}
	if q.Null && p.Null {
		return true
	}
	return q.ValString == p.ValString
}

func sameResult(db1, db2 *sql.DB, stmt string) error {
	var (
		wg   sync.WaitGroup
		res1 *sql.Rows
		res2 *sql.Rows
		err1 error
		err2 error
	)

	wg.Add(2)
	go func() {
		res1, err1 = db1.Query(stmt)
		wg.Done()
	}()
	go func() {
		res2, err2 = db2.Query(stmt)
		wg.Done()
	}()
	wg.Wait()

	if err1 != nil {
		return errors.Trace(err1)
	}
	if err2 != nil {
		return errors.Trace(err2)
	}

	cols1, err := res1.ColumnTypes()
	if err != nil {
		return errors.Trace(err)
	}
	cols2, err := res2.ColumnTypes()
	if err != nil {
		return errors.Trace(err)
	}

	var (
		cols1Anomaly []string
		cols2Anomaly []string
		m            = make(map[string]bool)
	)
	for _, c1 := range cols1 {
		m[c1.Name()] = false
	}
	for _, c2 := range cols2 {
		if _, ok := m[c2.Name()]; ok {
			m[c2.Name()] = true
		} else {
			cols2Anomaly = append(cols2Anomaly, c2.Name())
		}
	}
	for c1, b := range m {
		if !b {
			cols1Anomaly = append(cols1Anomaly, c1)
		}
	}

	if len(cols1Anomaly) != 0 || len(cols2Anomaly) != 0 {
		return errors.Errorf("columns not same, %v not in downstream, %v not in upstream", cols1Anomaly, cols2Anomaly)
	}

	var (
		result1 [][]*QueryItem
		result2 [][]*QueryItem
	)
	for res1.Next() {
		var (
			rowResultSets []interface{}
			resultRow     []*QueryItem
		)
		for range cols1 {
			rowResultSets = append(rowResultSets, new(interface{}))
		}
		if err := res1.Scan(rowResultSets...); err != nil {
			return errors.Trace(err)
		}
		for index, resultItem := range rowResultSets {
			r := *resultItem.(*interface{})
			item := QueryItem{
				ValType: cols1[index],
			}
			if r != nil {
				bytes := r.([]byte)
				item.ValString = string(bytes)
			} else {
				item.Null = true
			}
			resultRow = append(resultRow, &item)
		}
		result1 = append(result1, resultRow)
	}
	for res2.Next() {
		var (
			rowResultSets []interface{}
			resultRow     []*QueryItem
		)
		for range cols2 {
			rowResultSets = append(rowResultSets, new(interface{}))
		}
		if err := res2.Scan(rowResultSets...); err != nil {
			return errors.Trace(err)
		}
		for index, resultItem := range rowResultSets {
			r := *resultItem.(*interface{})
			item := QueryItem{
				ValType: cols2[index],
			}
			if r != nil {
				bytes := r.([]byte)
				item.ValString = string(bytes)
			} else {
				item.Null = true
			}
			resultRow = append(resultRow, &item)
		}
		result2 = append(result2, resultRow)
	}

	if len(result1) != len(result2) {
		return errors.Errorf("record count not same, up: %d, down: %d", len(result1), len(result2))
	}

	searched := make(map[int]struct{})
	width := len(cols1)
OUTER:
	for _, row1 := range result1 {
		for i, row2 := range result2 {
			if _, ok := searched[i]; ok {
				continue
			}
			same := true
			for j := 0; j < width; j++ {
				if !row1[j].Same(row2[j]) {
					same = false
					break
				}
			}
			if same {
				searched[i] = struct{}{}
				continue OUTER
			}
		}
		var b strings.Builder
		for i, item := range row1 {
			if i != 0 {
				b.WriteString(", ")
			}
			if item.Null {
				b.WriteString("NULL")
			} else {
				b.WriteString(item.ValString)
			}
		}
		return errors.Errorf("missing col in down: %s", b.String())
	}

	return nil
}

func check(db1, db2 *sql.DB) error {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	showIndexes := fmt.Sprintf("SHOW INDEXES FROM %s", tableName)
	checkTable := fmt.Sprintf("ADMIN CHECK TABLE %s", tableName)

	if _, err := db1.Exec(checkTable); err != nil {
		return errors.Trace(err)
	}

	if db2 == nil {
		return nil
	}

	if _, err := db2.Exec(checkTable); err != nil {
		return errors.Trace(err)
	}

	if err := sameResult(db1, db2, showIndexes); err != nil {
		return errors.Trace(err)
	}

	return sameResult(db1, db2, query)
}

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
	l.RLock()
	defer l.RUnlock()
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
	l.RLock()
	defer l.RUnlock()
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
	l.RUnlock()
	wg.Wait()
}
