package main

import (
	"strings"
	"sync"

	"github.com/you06/go-mikadzuki/util"
)

type UniqueIndex struct {
	indexes []int
	columns []ColumnType
	unique  *Unique
}

type UniqueSets struct {
	sync.Mutex
	indexes map[string]UniqueIndex
}

func NewUniqueSets() UniqueSets {
	return UniqueSets{
		indexes: make(map[string]UniqueIndex),
	}
}

func (u *UniqueSets) Reset() {
	u.Lock()
	defer u.Unlock()
	u.indexes = make(map[string]UniqueIndex)
}

func (u *UniqueSets) NewIndex(name string, cols []ColumnType) {
	u.Lock()
	defer u.Unlock()
	if _, ok := u.indexes[name]; ok {
		panic("duplicated index name")
	}
	unique := NewUnique(name, cols)
	u.indexes[name] = UniqueIndex{
		indexes: []int{},
		columns: cols,
		unique:  unique,
	}
}

// GetIndexesByCols get all indexes will affected by given columns
func (u *UniqueSets) GetIndexesByCols(cols []ColumnType) []*Unique {
	var uniques []*Unique
	colMap := make(map[string]struct{})
	for _, c := range cols {
		colMap[c.name] = struct{}{}
	}
OUTER:
	for _, index := range u.indexes {
		if index.unique.dropped {
			continue
		}
		for _, c := range index.columns {
			if _, ok := colMap[c.name]; ok {
				uniques = append(uniques, index.unique)
				continue OUTER
			}
		}
	}
	return uniques
}

func (u *UniqueSets) GetIndex(name string) *UniqueIndex {
	u.Lock()
	defer u.Unlock()
	ui, ok := u.indexes[name]
	if !ok {
		return nil
	}
	return &ui
}

// Unique is used for checking if inner state conflicted by unique keys
type Unique struct {
	sync.Mutex
	// dropped stands for whether this index has been dropped
	// do not need to check unique if it has been dropped successfuly
	dropped bool
	name    string
	cols    []ColumnType
	entries map[string]struct{}
}

func NewUnique(name string, cols []ColumnType) *Unique {
	return &Unique{
		name:    name,
		dropped: false,
		cols:    cols,
		entries: make(map[string]struct{}),
	}
}

func (u *Unique) row2key(row []interface{}) string {
	util.AssertEQ(len(u.cols), len(row))
	var b strings.Builder
	for i, col := range u.cols {
		if i != 0 {
			b.WriteString("~")
		}
		b.WriteString(col.tp.ToHashString(row[i]))
	}
	return b.String()
}

func (u *Unique) NewEntry(row []interface{}) bool {
	u.Lock()
	defer u.Unlock()
	if u.dropped {
		return true
	}
	entry := u.row2key(row)
	if _, ok := u.entries[entry]; ok {
		return false
	}
	u.entries[entry] = struct{}{}
	return true
}

func (u *Unique) HasConflict(befores, afters [][]interface{}) bool {
	for i := 0; i < len(befores); i++ {
		beforeEntry, afterEntry := u.row2key(befores[i]), u.row2key(afters[i])
		if beforeEntry == afterEntry {
			continue
		}
		if _, ok := u.entries[afterEntry]; ok {
			return true
		}
	}
	return false
}

func (u *Unique) UpdateEntry(befores, afters [][]interface{}) bool {
	for i := 0; i < len(befores); i++ {
		beforeEntry, afterEntry := u.row2key(befores[i]), u.row2key(afters[i])
		if beforeEntry == afterEntry {
			return true
		}
		if _, ok := u.entries[afterEntry]; ok {
			return false
		}
		delete(u.entries, beforeEntry)
		u.entries[afterEntry] = struct{}{}
	}
	return true
}
