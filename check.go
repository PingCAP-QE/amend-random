package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
)

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

	if err := sameResult(db1, db2, query); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func waitSync(db2 *sql.DB, ts int64) {
	sql := fmt.Sprintf("SELECT * FROM %s ORDER BY id DESC LIMIT 1", checkTableName)
	i := 0
	for {
		rows, err := db2.Query(sql)
		if err == nil {
			var r int64
			if !rows.Next() {
				rows.Close()
				continue
			}
			if rows.Scan(&r) != nil {
				continue
			}
			rows.Close()
			if ts == r {
				return
			}
		}
		i++
		if i%600 == 0 {
			fmt.Printf("sync for %d seconds\n", i/60)
		}
		if i > 10000 {
			fmt.Println("sync for 1000 seconds, skip waiting, check the drainer's status and log for details")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
