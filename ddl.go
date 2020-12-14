package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/you06/go-mikadzuki/kv"
	"github.com/you06/go-mikadzuki/util"
)

var (
	indexMutex       sync.Mutex
	indexSet         = make(map[string]struct{})
	uniqueIndexMutex sync.Mutex
	uniqueIndexSet   = make(map[string]struct{})
	uniqueSets       = NewUniqueSets()
)

type ddlRandom func(*[]ColumnType, *sql.DB, *Log, *sync.WaitGroup, *sync.WaitGroup, *sync.WaitGroup)

func CreateIndex(columns *[]ColumnType, db *sql.DB, log *Log, readyDMLWg, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	readyDMLWg.Done()
	threadName := "create-index"
	var unrelatedTblName string
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
			doErr := DoSomeUnrelatedDDLs(db, &unrelatedTblName, threadName, log)
			if doErr != nil {
				fmt.Println(fmt.Sprintf("unrelated ddl failed err=%v", doErr))
			}
		}
	}

	readyCommitWg.Done()
}

func DropIndex(columns *[]ColumnType, db *sql.DB, log *Log, readyDMLWg, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	readyDMLWg.Done()
	threadName := "drop-index"
	var unrelatedTblName string
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
			doErr := DoSomeUnrelatedDDLs(db, &unrelatedTblName, threadName, log)
			if doErr != nil {
				fmt.Println(fmt.Sprintf("unrelated ddl failed err=%v", doErr))
			}
		}
	}
	readyCommitWg.Done()
}

type IndexStmt struct {
	index string
	stmt  string
	cols  []ColumnType
}

func CreateUniqueIndex(columns *[]ColumnType, db *sql.DB, log *Log, readyDMLWg, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	threadName := "create-unique-index"
	var unrelatedTblName string
	util.AssertNil(log.NewThread(threadName))
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
	readyDMLWg.Done()
	readyDDLWg.Wait()
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
			doErr := DoSomeUnrelatedDDLs(db, &unrelatedTblName, threadName, log)
			if doErr != nil {
				fmt.Println(fmt.Sprintf("unrelated ddl failed err=%v", doErr))
			}
		}
	}
	readyCommitWg.Done()
}

func DropUniqueIndex(columns *[]ColumnType, db *sql.DB, log *Log, readyDMLWg, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	readyDMLWg.Done()
	threadName := "drop-unique-index"
	var unrelatedTblName string
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
			doErr := DoSomeUnrelatedDDLs(db, &unrelatedTblName, threadName, log)
			if doErr != nil {
				fmt.Println(fmt.Sprintf("unrelated ddl failed err=%v", doErr))
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

func AddColumn(columns *[]ColumnType, db *sql.DB, log *Log, readyDMLWg, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	readyDMLWg.Done()
	threadName := "add-column"
	util.AssertNil(log.NewThread(threadName))
	readyDDLWg.Wait()
	var unrelatedTblName string
	ddlNum := ddlCnt + util.RdRange(1, 10)
	for i := 0; i < ddlNum; i++ {
		rndType := kv.RdType()
		addColumn := NewColumnType(i, fmt.Sprintf("new_col_%d", i), rndType, rndType.Size(), util.RdBool())
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableName, addColumn.ToColStr())
		logIndex := log.Exec(threadName, stmt)
		if _, err := db.Exec(stmt); err != nil {
			log.Done(threadName, logIndex, err)
			fmt.Println(err)
		} else {
			log.Done(threadName, logIndex, nil)
			doErr := DoSomeUnrelatedDDLs(db, &unrelatedTblName, threadName, log)
			if doErr != nil {
				fmt.Println(fmt.Sprintf("unrelated ddl failed err=%v", doErr))
			}
		}
	}
	readyCommitWg.Done()
}

func DropColumn(columns *[]ColumnType, db *sql.DB, log *Log, readyDMLWg, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	readyDMLWg.Done()
	threadName := "drop-column"
	util.AssertNil(log.NewThread(threadName))
	readyDDLWg.Wait()
	colNum := len(*columns)
	leftIndex := 1
	var unrelatedTblName string
	if colCnt/2 > leftIndex {
		leftIndex = colCnt / 2
	}
	for i := colNum - 1; i >= leftIndex; i-- {
		dropColumn := (*columns)[i]
		stmt := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableName, dropColumn.name)
		logIndex := log.Exec(threadName, stmt)
		if _, err := db.Exec(stmt); err != nil {
			log.Done(threadName, logIndex, err)
			fmt.Println(err)
		} else {
			log.Done(threadName, logIndex, nil)
			doErr := DoSomeUnrelatedDDLs(db, &unrelatedTblName, threadName, log)
			if doErr != nil {
				fmt.Println(fmt.Sprintf("unrelated ddl failed err=%v", doErr))
			}
		}
	}
	readyCommitWg.Done()
}

func DoSomeUnrelatedDDLs(db *sql.DB, tableName *string, threadName string, log *Log) error {
	if len(*tableName) > 0 {
		err := DoUnrelatedDropTableDDL(db, *tableName, threadName, log)
		if err != nil {
			return err
		}
		*tableName = ""
	} else {
		err, name := DoUnrelatedCreateTableDDL(db, threadName, log)
		if err != nil {
			return err
		}
		*tableName = name
	}
	return nil
}

func DoUnrelatedCreateTableDDL(db *sql.DB, threadName string, log *Log) (error, string) {
	columns, primary := RdColumnsAndPk(5)
	tableName := fmt.Sprintf("t_%d", time.Now().UnixNano())
	createStmt := strings.ReplaceAll(GenCreateTableStmt(columns, primary, tableName), "\n", "")
	logIndex := log.Exec(threadName, createStmt)
	if _, err := db.Exec(createStmt); err != nil {
		log.Done(threadName, logIndex, err)
		fmt.Println(err)
		return err, ""
	}
	log.Done(threadName, logIndex, nil)
	return nil, tableName
}

func DoUnrelatedDropTableDDL(db *sql.DB, tableName string, threadName string, log *Log) error {
	dropStmt := GenDropTableStmt(tableName)
	logIndex := log.Exec(threadName, dropStmt)
	if _, err := db.Exec(dropStmt); err != nil {
		log.Done(threadName, logIndex, err)
		fmt.Println(err)
		return err
	}
	log.Done(threadName, logIndex, nil)
	return nil
}

var sizeIncrease = map[kv.DataType]kv.DataType{
	kv.TinyInt: kv.Int,
	kv.Int:     kv.BigInt,
	kv.Char:    kv.Char,
	kv.Varchar: kv.Varchar,
}

func ChangeColumnSize(columns *[]ColumnType, db *sql.DB, log *Log, readyDMLWg, readyDDLWg, readyCommitWg *sync.WaitGroup) {
	readyDMLWg.Done()
	threadName := "change-column-size"
	util.AssertNil(log.NewThread(threadName))
	readyDDLWg.Wait()

	ddlNum := ddlCnt + util.RdRange(1, 10)
	for i := 0; i < ddlNum; i++ {
		ok := false
		var (
			column *ColumnType
			newTp  kv.DataType
		)
		for !ok {
			column = &(*columns)[util.RdRange(0, len(*columns)/2)]
			newTp, ok = sizeIncrease[column.tp]
		}

		if column.tp == newTp {
			column.len += 1
		} else {
			column.tp = newTp
		}

		stmt := fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s %s", tableName, column.name, column.name, column.tp.String())
		if column.len > 0 {
			stmt += fmt.Sprintf("(%d)", column.len)
		}
		if !column.null {
			stmt += " NOT NULL"
		} else {
			stmt += " NULL"
		}

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

func ChangeColumnType(columns *[]ColumnType, db *sql.DB, log *Log, readyDMLWg, readyDDLWg, readyCommitWg *sync.WaitGroup) {

}
