package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/you06/go-mikadzuki/kv"
	"github.com/you06/go-mikadzuki/util"
)

var (
	columnLeast   = 2
	columnMost    = 200
	colCnt        = 0
	ddlCnt        = 10
	dmlCnt        = 10
	dmlThread     = 20
	dsn1          = ""
	dsn2          = ""
	mode          = ""
	selectedModes []string
	modeList      = []string{
		"create-index", "drop-index", "create-unique-index", "drop-unique-index", "add-column", "drop-column",
	}
	modeMap = map[string]ddlRandom{
		"create-index":        CreateIndex,
		"drop-index":          DropIndex,
		"create-unique-index": CreateUniqueIndex,
		"drop-unique-index":   DropUniqueIndex,
		"add-column":          AddColumn,
		"drop-column":         DropColumn,
	}
	modeFns   []ddlRandom
	tableName = "t"
)

func init() {
	flag.IntVar(&dmlCnt, "dml-count", 10, "dml count")
	flag.IntVar(&dmlThread, "dml-thread", 20, "dml thread")
	flag.StringVar(&dsn1, "dsn1", "root:@tcp(127.0.0.1:4000)/test?tidb_enable_amend_pessimistic_txn=1", "upstream dsn")
	flag.StringVar(&dsn2, "dsn2", "", "downstream dsn")
	flag.StringVar(&mode, "mode", "", fmt.Sprintf("ddl modes, split with \",\", supportted modes: %s", strings.Join(modeList, ",")))

	rand.Seed(time.Now().UnixNano())
	flag.Parse()
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
		return errors.New("no sql mode is selected")
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

	MustExec(db1, "DROP TABLE IF EXISTS check_points")
	MustExec(db1, "CREATE TABLE check_points(id int)")
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

func rdColumns() []ColumnType {
	colCnt = util.RdRange(columnLeast, columnMost)
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
	columns := rdColumns()
	columns[0].null = false
	primary := []ColumnType{columns[0]}
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

	// dml threads
	for i := 0; i < dmlThread; i++ {
		go func(i int) {
			readyDMLWg.Wait()
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
			doneInsertWg.Done()
			doneInsertWg.Wait()
			for i := 0; i < dmlCnt; i++ {
				stmt, cond, cols := updateBatchSQL(columns)
				logIndex := log.Exec(threadName, stmt)
				err := updateIfNotConflict(txn, stmt, cond, cols)
				if err != nil {
					log.Done(threadName, logIndex, err)
					fmt.Println(err)
					if strings.Contains(err.Error(), "Lock wait timeout exceeded") ||
						strings.Contains(err.Error(), "Deadlock found ") {
						wg.Done()
						return
					}
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
		now := time.Now().Unix()
		MustExec(db, fmt.Sprintf("INSERT INTO check_points VALUES(%d)", now))
		fmt.Println("wait for sync")
		waitSync(db2, now)
		fmt.Println("ready to check")
	}
	return check(db, db2)
}
