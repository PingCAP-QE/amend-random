package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/you06/go-mikadzuki/kv"
	"github.com/you06/go-mikadzuki/util"
)

func insertSQL(columns []ColumnType, count int) (string, [][]interface{}) {
	var (
		b    strings.Builder
		data = make([][]interface{}, count)
	)
	fmt.Fprintf(&b, "INSERT INTO %s VALUES", tableName)
	for i := 0; i < count; i++ {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		row := make([]interface{}, len(columns))
		for j, column := range columns {
			util.AssertEQ(column.i, j)
			if j != 0 {
				b.WriteString(", ")
			}
			v := column.tp.RandValue()
			b.WriteString(column.tp.ValToString(v))
			row[j] = v
		}
		data[i] = row
		b.WriteString(")")
	}
	return b.String(), data
}

func updateBatchSQL(columns []ColumnType, data [][]interface{}) (string, string, []ColumnType) {
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

func updateIfNotConflict(txn *sql.Tx, updateStmt, selectForUpdateCond string, columns []ColumnType) error {
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
				unique.UpdateEntry(befores[i], afters[i])
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
		return before.(int) / 2
	case kv.Date, kv.Datetime, kv.Timestamp:
		return before.(time.Time).Add(24 * time.Hour)
	case kv.Char, kv.Varchar, kv.Text:
		return before.(string) + "-p"
	default:
		panic(fmt.Sprintf("tp %s not supported", tp.String()))
	}
}
