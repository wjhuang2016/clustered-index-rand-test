package sqlgen_test

import (
	"fmt"
	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	. "github.com/pingcap/check"
)

func generateCreateTable(state *sqlgen.State, tblCount, colCount, idxCount int) []string {
	result := make([]string, 0, tblCount)
	state.SetRepeat(sqlgen.ColumnDefinition, colCount, colCount)
	state.SetRepeat(sqlgen.IndexDefinition, idxCount, idxCount)
	for i := 0; i < tblCount; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		result = append(result, sql)
	}
	return result
}

func generateInsertInto(state *sqlgen.State, rowCount int) []string {
	result := make([]string, 0, rowCount)
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < rowCount; i++ {
			sql := sqlgen.InsertInto.Eval(state)
			result = append(result, sql)
		}
		state.DestroyScope()
	}
	return result
}

func generateQuery(state *sqlgen.State, count int) []string {
	result := make([]string, 0, count)
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < count; i++ {
			sql := sqlgen.Query.Eval(state)
			result = append(result, sql)
		}
		state.DestroyScope()
	}
	return result
}

func (s *testSuite) TestQuery(c *C) {
	state := sqlgen.NewState()
	rowCount := 10
	tblCount := 2
	for i := 0; i < tblCount; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Println(sql)
	}
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < rowCount; i++ {
			sql := sqlgen.InsertInto.Eval(state)
			fmt.Println(sql)
		}
		state.DestroyScope()
	}
	queries := generateQuery(state, rowCount)
	for _, sql := range queries {
		fmt.Println(sql)
	}
}

func (s *testSuite) TestExampleInitialize(c *C) {
	state := sqlgen.NewState()
	tableCount, columnCount := 5, 5
	indexCount, rowCount := 2, 10
	initSQLs := generateCreateTable(state, tableCount, columnCount, indexCount)
	for _, sql := range initSQLs {
		fmt.Println(sql)
	}
	insertSQLs := generateInsertInto(state, rowCount)
	for _, sql := range insertSQLs {
		fmt.Println(sql)
	}
}
