package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/you06/go-mikadzuki/kv"
	"github.com/you06/go-mikadzuki/util"
)

type dmlExecutorOption struct {
	dmlCnt        int
	dmlThread     int
	readyDMLWg    *sync.WaitGroup
	readyDDLWg    *sync.WaitGroup
	readyCommitWg *sync.WaitGroup
	doneWg        *sync.WaitGroup
}

type DMLExecutor func(*[]ColumnType, *sql.DB, *Log, dmlExecutorOption)

func EmptyExecutor(columns *[]ColumnType, db *sql.DB, log *Log, opt dmlExecutorOption) {
	for i := 0; i < opt.dmlThread; i++ {
		go func(i int) {
			opt.readyDMLWg.Wait() // wait for init
			// begin transaction
			opt.readyDDLWg.Done() // ddl will be executed after this
			// dmls
			opt.readyCommitWg.Wait() // wait for ddl done
			// commit transaction
			opt.doneWg.Done()
		}(i)
	}
}

func UpdateConflictExecutor(columns *[]ColumnType, db *sql.DB, log *Log, opt dmlExecutorOption) {
	var (
		dmlCnt        = opt.dmlCnt
		dmlThread     = opt.dmlThread
		readyDMLWg    = opt.readyDMLWg
		readyDDLWg    = opt.readyDDLWg
		readyCommitWg = opt.readyCommitWg
		doneWg        = opt.doneWg
	)
	var doneInsertWg sync.WaitGroup
	var (
		totalCommitDuration int64
		doneThread          int64
	)
	doneInsertWg.Add(dmlThread)

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

			insertStmt := insertSQL(*columns, batchSize)
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
				stmt, cond, cols := updateBatchSQL(*columns)
				logIndex := log.Exec(threadName, stmt)
				err := updateIfNotConflict(txn, stmt, cond, cols, false)
				if err != nil {
					log.Done(threadName, logIndex, err)
					fmt.Println(err)
					if breakTxn(err) {
						doneWg.Done()
						return
					}
				} else {
					log.Done(threadName, logIndex, nil)
				}
			}
			readyCommitWg.Wait()
			logIndex = log.Exec(threadName, "COMMIT")
			startCommitTS := time.Now()
			if err := txn.Commit(); err != nil {
				log.Done(threadName, logIndex, err)
			} else {
				log.Done(threadName, logIndex, nil)
			}
			commitDuration := time.Since(startCommitTS).Seconds()
			atomic.AddInt64(&totalCommitDuration, int64(commitDuration))
			atomic.AddInt64(&doneThread, 1)
			doneWg.Done()
		}(i)
	}

	go func() {
		doneWg.Wait()
		if doneThread == 0 {
			fmt.Println("all transaction failed")
			return
		}
		avgCommitDuration := totalCommitDuration / doneThread
		fmt.Printf("avg commit duration %ds\n", avgCommitDuration)
	}()
}

func InsertUpdateExecutor(columns *[]ColumnType, db *sql.DB, log *Log, opt dmlExecutorOption) {
	var (
		dmlCnt        = opt.dmlCnt
		dmlThread     = opt.dmlThread
		readyDMLWg    = opt.readyDMLWg
		readyDDLWg    = opt.readyDDLWg
		readyCommitWg = opt.readyCommitWg
		doneWg        = opt.doneWg
	)
	var (
		totalCommitDuration int64
		doneThread          int64
	)
	var doneInsertWg sync.WaitGroup
	doneInsertWg.Add(dmlThread)

	if txnSize != 0 {
		rowSize := RowSize(*columns)
		rowCnt := txnSize / int64(rowSize)
		batchSize = int(rowCnt) / 1000
		if batchSize > 500 {
			batchSize = 500
		}
		dmlCnt = int(rowCnt) / batchSize
		fmt.Printf("%s will be trans to %d dmls\n", txnSizeStr, dmlCnt)
	}

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

			for j := 0; j < dmlCnt/2; j++ {
				insertStmt := insertSQL(*columns, batchSize)
				logIndex = log.Exec(threadName, insertStmt)
				_, err = txn.Exec(insertStmt)
				if err != nil {
					log.Done(threadName, logIndex, err)
					fmt.Println(err)
					if breakTxn(err) {
						doneInsertWg.Done()
						doneWg.Done()
						return
					}
				} else {
					log.Done(threadName, logIndex, nil)
				}
			}

			doneInsertWg.Done()
			doneInsertWg.Wait()

			// stmt, cond, cols := updateBatchSQL(*columns)
			// logIndex = log.Exec(threadName, stmt)
			// err = updateIfNotConflict(txn, stmt, cond, cols)
			// if err != nil {
			// 	log.Done(threadName, logIndex, err)
			// 	fmt.Println(err)
			// 	if strings.Contains(err.Error(), "Lock wait timeout exceeded") ||
			// 		strings.Contains(err.Error(), "Deadlock found ") {
			// 		doneWg.Done()
			// 		return
			// 	}
			// } else {
			// 	log.Done(threadName, logIndex, nil)
			// }

			for j := 0; j < dmlCnt/2; j++ {
				insertStmt := insertSQL(*columns, batchSize)
				logIndex = log.Exec(threadName, insertStmt)
				_, err = txn.Exec(insertStmt)
				if err != nil {
					log.Done(threadName, logIndex, err)
					fmt.Println(err)
					if breakTxn(err) {
						doneWg.Done()
						return
					}
				} else {
					log.Done(threadName, logIndex, nil)
				}
			}

			stmt, cond, cols := updateBatchSQL(*columns)
			logIndex = log.Exec(threadName, stmt)
			err = updateIfNotConflict(txn, stmt, cond, cols, true)
			if err != nil {
				log.Done(threadName, logIndex, err)
				fmt.Println(err)
				if breakTxn(err) {
					doneWg.Done()
					return
				}
			} else {
				log.Done(threadName, logIndex, nil)
			}

			readyCommitWg.Wait()
			logIndex = log.Exec(threadName, "COMMIT")
			startCommitTS := time.Now()
			if err := txn.Commit(); err != nil {
				log.Done(threadName, logIndex, err)
			} else {
				log.Done(threadName, logIndex, nil)
			}
			commitDuration := time.Since(startCommitTS).Seconds()
			atomic.AddInt64(&totalCommitDuration, int64(commitDuration))
			atomic.AddInt64(&doneThread, 1)
			doneWg.Done()
		}(i)
	}

	go func() {
		doneWg.Wait()
		if doneThread == 0 {
			fmt.Println("all transaction failed")
			return
		}
		avgCommitDuration := totalCommitDuration / doneThread
		fmt.Printf("avg commit duration %ds\n", avgCommitDuration)
	}()
}

const RETRY_COUNT = 100

func insertSQL(columns []ColumnType, count int) string {
	var (
		b strings.Builder
	)
	fmt.Fprintf(&b, "INSERT INTO %s VALUES", tableName)
	uniqueSets.Lock()
	uniques := uniqueSets.GetAllIndexes()
	for _, unique := range uniques {
		unique.Lock()
	}
	defer func() {
		for _, unique := range uniques {
			unique.Unlock()
		}
		uniqueSets.Unlock()
	}()

	for i := 0; i < count; i++ {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		row := make([]interface{}, len(columns))
	GENERATE:
		for tryTime := 0; true; tryTime++ {
			for j, column := range columns {
				util.AssertEQ(column.i, j)
				row[j] = column.tp.RandValue()
			}
			entries := make([][]interface{}, len(uniques))
			for j, unique := range uniques {
				indexRow := make([]interface{}, len(unique.cols))
				for k, c := range unique.cols {
					indexRow[k] = row[c.i]
				}
				if unique.HasConflictEntry(indexRow) {
					fmt.Println("conflict retry time:", tryTime)
					fmt.Println("index name:", unique.name, "data type:", unique.cols,"value:", indexRow, "conflict")
					if tryTime >= RETRY_COUNT {
						break GENERATE
					}
					continue GENERATE
				}
				entries[j] = indexRow
			}
			for j, entry := range entries {
				uniques[j].NewEntry(entry)
			}
			break GENERATE
		}
		for j, column := range columns {
			if j != 0 {
				b.WriteString(", ")
			}
			b.WriteString(column.tp.ValToString(row[j]))
		}
		b.WriteString(")")
	}
	return b.String()
}

func updateBatchSQL(columns []ColumnType) (string, string, []ColumnType) {
	var (
		b               strings.Builder
		selectForUpdate strings.Builder
		cols            []ColumnType
	)

	fmt.Fprintf(&b, "UPDATE %s SET ", tableName)
	for i := 0; i < util.RdRange(1, 5); i++ {
		if i != 0 {
			b.WriteString(", ")
		}

		column := columns[util.RdRange(0, len(columns))]
		cols = append(cols, column)
		switch column.tp {
		case kv.Int, kv.BigInt, kv.TinyInt:
			fmt.Fprintf(&b, "%s=%s/2", column.name, column.name)
		case kv.Date, kv.Datetime, kv.Timestamp:
			fmt.Fprintf(&b, "%s = ADDDATE(%s, INTERVAL 1 DAY)", column.name, column.name)
		case kv.Char, kv.Varchar, kv.Text:
			fmt.Fprintf(&b, "%s = CONCAT(%s, \"-p\")", column.name, column.name)
		}
	}

	b.WriteString(" WHERE ")
	selectForUpdate.WriteString(" WHERE ")
	for i := 0; i < util.RdRange(1, 5); i++ {
		if i != 0 {
			b.WriteString(" AND ")
			selectForUpdate.WriteString(" AND ")
		}

		col := columns[util.RdRange(0, len(columns))]

		fmt.Fprintf(&b, "%s<%s", col.name, col.tp.ValToString(col.tp.RandValue()))
		fmt.Fprintf(&selectForUpdate, "%s<%s", col.name, col.tp.ValToString(col.tp.RandValue()))
	}

	return b.String(), selectForUpdate.String(), cols
}

func updateIfNotConflict(txn *sql.Tx, updateStmt, selectForUpdateCond string, columns []ColumnType, keepOldEntry bool) error {
	uniqueSets.Lock()
	uniques := uniqueSets.GetIndexesByCols(columns)
	if len(uniques) > 0 {
		var (
			b      strings.Builder
			cols   []ColumnType
			colMap = make(map[string]struct{})
		)

		b.WriteString("SELECT ")

		for _, c := range columns {
			colMap[c.name] = struct{}{}
		}

		for i, unique := range uniques {
			unique.Lock()
			for j, c := range unique.cols {
				if i != 0 || j != 0 {
					b.WriteString(", ")
				}
				cols = append(cols, c)
				b.WriteString(c.name)
			}
		}
		uniqueSets.Unlock()
		defer func() {
			for _, unique := range uniques {
				unique.Unlock()
			}
		}()

		fmt.Fprintf(&b, " FROM %s ", tableName)
		b.WriteString(selectForUpdateCond)
		forUpdateData, err := query(txn, cols, b.String())
		if err != nil {
			return err
		}

		index := 0
		var (
			befores = make([][][]interface{}, len(uniques))
			afters  = make([][][]interface{}, len(uniques))
		)
		for i, unique := range uniques {
			var (
				before = make([][]interface{}, len(forUpdateData))
				after  = make([][]interface{}, len(forUpdateData))
			)
			for j := 0; j < len(forUpdateData); j++ {
				before[j] = make([]interface{}, len(unique.cols))
				after[j] = make([]interface{}, len(unique.cols))
				for k, column := range unique.cols {
					before[j][k] = forUpdateData[j][index+k]
					after[j][k] = updateItem(forUpdateData[j][index+k], column.tp)
				}
			}
			if unique.HasConflict(before, after) {
				return nil
			}
			befores[i] = before
			afters[i] = after
			index += len(unique.cols)
		}
		// no conflict, can execute
		_, err = txn.Exec(updateStmt)
		if err == nil {
			for i, unique := range uniques {
				unique.UpdateEntry(befores[i], afters[i], keepOldEntry)
			}
		}
		return err
	} else {
		uniqueSets.Unlock()
		return nil
	}
}

func query(txn *sql.Tx, columns []ColumnType, stmt string) ([][]interface{}, error) {
	rows, err := txn.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var data [][]interface{}
	for rows.Next() {
		rowData := make([]interface{}, len(columns))
		rowPointers := make([]interface{}, len(columns))
		for i, column := range columns {
			switch column.tp {
			case kv.Int, kv.BigInt, kv.TinyInt:
				rowPointers[i] = new(int)
			case kv.Date, kv.Datetime, kv.Timestamp:
				rowPointers[i] = new(string)
			case kv.Char, kv.Varchar, kv.Text:
				rowPointers[i] = new(string)
			}
		}
		if err := rows.Scan(rowPointers...); err != nil {
			return nil, err
		}
		for i, column := range columns {
			switch column.tp {
			case kv.Int, kv.BigInt, kv.TinyInt:
				rowData[i] = *rowPointers[i].(*int)
			case kv.Date:
				rowData[i], _ = time.Parse("2006-01-02", *rowPointers[i].(*string))
			case kv.Datetime, kv.Timestamp:
				rowData[i], _ = time.Parse("2006-01-02 15:04:05", *rowPointers[i].(*string))
			case kv.Char, kv.Varchar, kv.Text:
				rowData[i] = *rowPointers[i].(*string)
			}
		}
		data = append(data, rowData)
	}
	return data, nil
}

func updateItem(before interface{}, tp kv.DataType) interface{} {
	switch tp {
	case kv.Int, kv.BigInt, kv.TinyInt:
		i := before.(int)
		// special rules in MySQL for odd negative number
		// 85 / 2 = 42
		// -85 / 2 = -43
		if i >= 0 || i%2 == 0 {
			return i / 2
		} else {
			return i/2 - 1
		}
	case kv.Date, kv.Datetime, kv.Timestamp:
		return before.(time.Time).Add(24 * time.Hour)
	case kv.Char, kv.Varchar, kv.Text:
		return before.(string) + "-p"
	default:
		panic(fmt.Sprintf("tp %s not supported", tp.String()))
	}
}

func breakTxn(err error) bool {
	if strings.Contains(err.Error(), "Lock wait timeout exceeded") ||
		strings.Contains(err.Error(), "Deadlock found ") ||
		strings.Contains(err.Error(), "TTL manager has timed out") {
		return true
	}
	return false
}

func RowSize(row []ColumnType) int {
	s := 0
	for _, c := range row {
		s += ColSize(&c)
	}
	return s
}

// ColSize get the byte number of a column
func ColSize(col *ColumnType) int {
	switch col.tp {
	case kv.TinyInt:
		return 1
	case kv.Int:
		return 4
	case kv.BigInt:
		return 8
	case kv.Date:
		return 3
	case kv.Datetime:
		return 8
	case kv.Timestamp:
		return 4
	case kv.Char:
		return col.len
	case kv.Varchar:
		return 1 + col.len
	default:
		panic(fmt.Sprintf("unexpected type %s", col.tp))
	}
}
