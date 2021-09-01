package check

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

func SameRow(row1, row2 []*QueryItem) bool {
	if len(row1) != len(row2) {
		return false
	}
	for i := 0; i < len(row1); i++ {
		if !row1[i].Same(row2[i]) {
			return false
		}
	}
	return true
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
	defer func() {
		res1.Close()
	}()
	if err2 != nil {
		return errors.Trace(err2)
	}
	defer func() {
		res2.Close()
	}()

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
	fmt.Println("record count, up: %d, down: %d", len(result1), len(result2))
	if len(result1) != len(result2) {
		return errors.Errorf("record count not same, up: %d, down: %d", len(result1), len(result2))
	}

	return rowsSame(result1, result2)
}

func rowsSame(result1, result2 [][]*QueryItem) error {
	col0Map := make(map[string][][]*QueryItem)
	nulls := [][]*QueryItem{} // there should be 0 nulls

	for _, row := range result1 {
		if row[0].Null {
			nulls = append(nulls, row)
		} else {
			rows, ok := col0Map[row[0].ValString]
			if !ok {
				rows = [][]*QueryItem{row}
			} else {
				rows = append(rows, row)
			}
			col0Map[row[0].ValString] = rows
		}
	}

OUTER:
	for _, row2 := range result2 {
		if row2[0].Null {
			for i, row1 := range nulls {
				if SameRow(row1, row2) {
					nulls = append(nulls[:i], nulls[i+1:]...)
					continue OUTER
				}
			}
			return errRowNotFound(row2)
		} else {
			rows, ok := col0Map[row2[0].ValString]
			if !ok {
				return errRowNotFound(row2)
			}
			for i, row1 := range rows {
				if SameRow(row1, row2) {
					rows = append(rows[:i], rows[i+1:]...)
					col0Map[row2[0].ValString] = rows
					continue OUTER
				}
			}
			return errRowNotFound(row2)
		}
	}

	return nil
}

func errRowNotFound(row []*QueryItem) error {
	var b strings.Builder
	for i, item := range row {
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

func Check(db1, db2 *sql.DB, tableName string) error {
	start := time.Now()
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

	fmt.Println("compare index")
	if err := sameResult(db1, db2, showIndexes); err != nil {
		return errors.Trace(err)
	}

	fmt.Println("compare record")
	if err := sameResult(db1, db2, query); err != nil {
		return errors.Trace(err)
	}
	fmt.Printf("check pass in %ds\n", int(time.Since(start).Seconds()))
	return nil
}

func WaitSync(db2 *sql.DB, ts int64, checkTableName string) {
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
