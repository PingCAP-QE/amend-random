package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/you06/amend-random/check"
	"github.com/you06/go-mikadzuki/kv"
	"github.com/you06/go-mikadzuki/util"
)

var (
	columnLeast     = 2
	columnMost      = 200
	colCnt          = 0
	ddlCnt          = 10
	dmlCnt          = 10
	dmlThread       = 20
	batchSize       = 10
	totalRound      = 0
	txnSizeStr      = ""
	txnSize         int64
	dsn1            = ""
	dsn2            = ""
	mode            = ""
	dmlExecutorName = ""
	dmlExecutor     DMLExecutor
	selectedModes   []string
	modeMap         = map[string]ddlRandom{
		"create-index":        CreateIndex,
		"drop-index":          DropIndex,
		"create-unique-index": CreateUniqueIndex,
		"drop-unique-index":   DropUniqueIndex,
		"add-column":          AddColumn,
		"drop-column":         DropColumn,
	}
	dmlExecutors = map[string]DMLExecutor{
		"update-conflict": UpdateConflictExecutor,
		"insert-update":   InsertUpdateExecutor,
	}
	modeFns        []ddlRandom
	tableName      = "t"
	checkTableName = ""
	checkOnly      bool
	mbSize         = 1048576
)

func init() {
	var (
		supportedMode     []string
		supportedExecutor []string
	)
	for k := range modeMap {
		supportedMode = append(supportedMode, k)
	}
	for k := range dmlExecutors {
		supportedExecutor = append(supportedExecutor, k)
	}
	flag.IntVar(&dmlCnt, "dml-count", 10, "dml count")
	flag.IntVar(&dmlThread, "dml-thread", 20, "dml thread")
	flag.StringVar(&dsn1, "dsn1", "root:@tcp(127.0.0.1:4000)/test?tidb_enable_amend_pessimistic_txn=1", "upstream dsn")
	flag.StringVar(&dsn2, "dsn2", "", "downstream dsn")
	flag.StringVar(&mode, "mode", "", fmt.Sprintf("ddl modes, split with \",\", supportted modes: %s", strings.Join(supportedMode, ",")))
	flag.StringVar(&dmlExecutorName, "executor", supportedExecutor[0], fmt.Sprintf("dml executor, supportted executors: %s", strings.Join(supportedExecutor, ",")))
	flag.StringVar(&tableName, "tablename", "t", "tablename")
	flag.BoolVar(&checkOnly, "checkonly", false, "only check diff")
	flag.StringVar(&txnSizeStr, "txn-size", "", "the estimated txn's size, will overwrite dml-count, eg. 100M, 1G")
	flag.IntVar(&totalRound, "round", 0, "exec round, 0 means infinite execution")
	flag.IntVar(&batchSize, "batch", 10, "batch size of insert, 0 for auto")

	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	checkTableName = fmt.Sprintf("check_point_%s", tableName)
}

func initMode() error {
	selectedModes = strings.Split(mode, ",")
	set := make(map[string]struct{})
	for i, m := range selectedModes {
		m = strings.TrimSpace(m)
		fn, ok := modeMap[m]
		if !ok {
			return errors.Errorf("mode %s not supportted", m)
		}

		selectedModes[i] = m
		if _, ok := set[m]; !ok {
			modeFns = append(modeFns, fn)
		}
		set[m] = struct{}{}
	}
	if len(modeFns) == 0 {
		fmt.Println("[WARN] no sql mode is selected, there will be DML only")
	}
	if txnSizeStr != "" {
		var err error
		txnSize, err = units.RAMInBytes(txnSizeStr)
		if err != nil {
			return err
		}
	}
	if e, ok := dmlExecutors[dmlExecutorName]; !ok {
		return errors.Errorf("invalid dml executor name `%s`", dmlExecutorName)
	} else {
		dmlExecutor = e
	}
	return nil
}

func main() {
	if err := initMode(); err != nil {
		panic(err)
	}

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

	if checkOnly {
		if err := check.Check(db1, db2, tableName); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("check pass, data same.")
		}
		return
	}

	MustExec(db1, fmt.Sprintf("DROP TABLE IF EXISTS %s", checkTableName))
	MustExec(db1, fmt.Sprintf("CREATE TABLE %s(id int)", checkTableName))
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
		if totalRound != 0 && totalRound >= round {
			return
		}
	}
}

type ColumnType struct {
	i    int
	name string
	tp   kv.DataType
	len  int
	null bool
}

func (c *ColumnType) ToColStr() string {
	var b strings.Builder
	if c.len > 0 {
		fmt.Fprintf(&b, "%s %s(%d)", c.name, c.tp, c.len)
	} else {
		fmt.Fprintf(&b, "%s %s", c.name, c.tp)
	}
	if !c.null {
		b.WriteString(" NOT")
	}
	b.WriteString(" NULL")
	return b.String()
}

func NewColumnType(i int, name string, tp kv.DataType, len int, null bool) ColumnType {
	return ColumnType{
		i:    i,
		name: name,
		tp:   tp,
		len:  len,
		null: null,
	}
}

func rdColumns(least int) []ColumnType {
	colCnt = util.RdRange(columnLeast, columnMost)
	if colCnt < least {
		colCnt = least
	}
	columns := make([]ColumnType, colCnt)

	for i := 0; i < colCnt; i++ {
		tp := kv.RdType()
		columns[i] = NewColumnType(i, fmt.Sprintf("col_%d", i), tp, tp.Size(), util.RdBool())
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

func once(db, db2 *sql.DB, log *Log) error {
	uniqueSets.Reset()
	indexSet = make(map[string]struct{})
	uniqueIndexSet = make(map[string]struct{})
	leastCol := 0
	if txnSize >= 200*mbSize {
		leastCol = 100
	}
	columns := rdColumns(leastCol)
	columns[0].null = false
	primary := []ColumnType{columns[0]}
	for pi := 1; columns[pi-1].tp == kv.TinyInt || pi <= 2; pi++ {
		columns[pi].null = false
		primary = append(primary, columns[pi])
	}
	uniqueSets.NewIndex("primary", primary)
	initThreadName := "init"
	clearTableStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	createTableStmt := createTable(columns, primary)

	util.AssertNil(log.NewThread(initThreadName))

	initLogIndex := log.Exec(initThreadName, clearTableStmt)
	MustExec(db, clearTableStmt)
	log.Done(initThreadName, initLogIndex, nil)

	initLogIndex = log.Exec(initThreadName, createTableStmt)
	MustExec(db, createTableStmt)
	log.Done(initThreadName, initLogIndex, nil)

	var (
		wg            sync.WaitGroup
		readyDMLWg    sync.WaitGroup
		readyDDLWg    sync.WaitGroup
		doneInsertWg  sync.WaitGroup
		readyCommitWg sync.WaitGroup
	)
	wg.Add(dmlThread)
	doneInsertWg.Add(dmlThread)
	readyDDLWg.Add(dmlThread)
	readyDMLWg.Add(len(modeFns))
	readyCommitWg.Add(len(modeFns))

	for _, fn := range modeFns {
		go fn(&columns, db, log, &readyDMLWg, &readyDDLWg, &readyCommitWg)
	}

	dmlExecutor(&columns, db, log, dmlExecutorOption{
		dmlCnt:        dmlCnt,
		dmlThread:     dmlThread,
		readyDMLWg:    &readyDMLWg,
		readyDDLWg:    &readyDDLWg,
		readyCommitWg: &readyCommitWg,
		doneWg:        &wg,
	})

	wg.Wait()

	if db2 != nil {
		now := time.Now().Unix()
		MustExec(db, fmt.Sprintf("INSERT INTO %s VALUES(%d)", checkTableName, now))
		fmt.Println("wait for sync")
		check.WaitSync(db2, now, checkTableName)
		fmt.Println("ready to check")
		// since the txn's order between tables is not garuantted, we wait extra 10 seconds
		time.Sleep(10 * time.Second)
	}
	return check.Check(db, db2, tableName)
}
