package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/you06/go-mikadzuki/util"
)

var (
	indexMutex       sync.Mutex
	indexSet         = make(map[string]struct{})
	uniqueIndexMutex sync.Mutex
	uniqueIndexSet   = make(map[string]struct{})
	uniqueSets       = NewUniqueSets()
)

type ddlRandom func(*[]ColumnType, *sql.DB, *Log, *sync.WaitGroup, *sync.WaitGroup)

func CreateIndex(columns *[]ColumnType, db *sql.DB, log *Log, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	threadName := "create-index"
	util.AssertNil(log.NewThread(threadName))
	readyDDLWg.Wait()
	for i := 0; i < ddlCnt; i++ {
		index, stmt := addIndex(*columns, i)
		logIndex := log.Exec(threadName, stmt)
		if _, err := db.Exec(stmt); err != nil {
			log.Done(threadName, logIndex, err)
			fmt.Println(err)
		} else {
			log.Done(threadName, logIndex, nil)
			indexMutex.Lock()
			indexSet[index] = struct{}{}
			indexMutex.Unlock()
		}
	}

	readyCommitWg.Done()
}

func DropIndex(columns *[]ColumnType, db *sql.DB, log *Log, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	threadName := "drop-index"
	util.AssertNil(log.NewThread(threadName))
	readyDDLWg.Wait()
	for i := 0; i < ddlCnt/2; i++ {
		_, stmt := dropIndex()
		logIndex := log.Exec(threadName, stmt)
		if _, err := db.Exec(stmt); err != nil {
			log.Done(threadName, logIndex, err)
			fmt.Println(err)
		} else {
			log.Done(threadName, logIndex, nil)
		}
	}
	readyCommitWg.Done()
}

type IndexStmt struct {
	index string
	stmt  string
	cols  []ColumnType
}

func CreateUniqueIndex(columns *[]ColumnType, db *sql.DB, log *Log, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	threadName := "create-unique-index"
	util.AssertNil(log.NewThread(threadName))
	readyDDLWg.Wait()
	stmts := make([]IndexStmt, ddlCnt)
	for i := 0; i < ddlCnt; i++ {
		index, stmt, cols := addUniqueIndex(*columns, i)
		stmts[i] = IndexStmt{
			index: index,
			stmt:  stmt,
			cols:  cols,
		}
		uniqueSets.NewIndex(index, cols)
	}
	for i := 0; i < ddlCnt; i++ {
		indexStmt := stmts[i]
		logIndex := log.Exec(threadName, indexStmt.stmt)
		if _, err := db.Exec(indexStmt.stmt); err != nil {
			log.Done(threadName, logIndex, err)
			fmt.Println(err)
		} else {
			log.Done(threadName, logIndex, nil)
			uniqueIndexMutex.Lock()
			uniqueIndexSet[indexStmt.index] = struct{}{}
			uniqueIndexMutex.Unlock()
		}
	}
	readyCommitWg.Done()
}

func DropUniqueIndex(columns *[]ColumnType, db *sql.DB, log *Log, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	threadName := "drop-unique-index"
	util.AssertNil(log.NewThread(threadName))
	readyDDLWg.Wait()
	for i := 0; i < ddlCnt/2; i++ {
		index, stmt := dropUniqueIndex()
		logIndex := log.Exec(threadName, stmt)
		if _, err := db.Exec(stmt); err != nil {
			log.Done(threadName, logIndex, err)
			fmt.Println(err)
		} else {
			log.Done(threadName, logIndex, nil)
			uniqueIndex := uniqueSets.GetIndex(index)
			if uniqueIndex != nil {
				uniqueIndex.unique.dropped = true
			}
		}
	}
	readyCommitWg.Done()
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

	indexMutex.Lock()
	for len(indexSet) == 0 {
		indexMutex.Unlock()
		time.Sleep(100 * time.Millisecond)
		indexMutex.Lock()
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
	indexMutex.Unlock()

	fmt.Fprintf(&b, "ALTER TABLE %s DROP INDEX %s", tableName, indexName)

	return indexName, b.String()
}

func addUniqueIndex(columns []ColumnType, i int) (string, string, []ColumnType) {
	var b strings.Builder
	indexes := make(map[int]struct{})
	cols := make([]ColumnType, 0, 5)
	indexName := fmt.Sprintf("u%d", i)

	fmt.Fprintf(&b, "CREATE UNIQUE INDEX %s on %s(", indexName, tableName)
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
		cols = append(cols, col)
		fmt.Fprintf(&b, "`%s`", col.name)
	}
	b.WriteString(")")

	return indexName, b.String(), cols
}

func dropUniqueIndex() (string, string) {
	var (
		indexName string
		b         strings.Builder
	)

	uniqueIndexMutex.Lock()
	for len(uniqueIndexSet) == 0 {
		uniqueIndexMutex.Unlock()
		time.Sleep(100 * time.Millisecond)
		uniqueIndexMutex.Lock()
	}
	l := util.RdRange(0, len(uniqueIndexSet))
	for index := range uniqueIndexSet {
		if l == 0 {
			indexName = index
			break
		}
		l--
	}
	delete(uniqueIndexSet, indexName)
	uniqueIndexMutex.Unlock()

	fmt.Fprintf(&b, "ALTER TABLE %s DROP INDEX %s", tableName, indexName)

	return indexName, b.String()
}
