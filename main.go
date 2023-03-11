package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	_ "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit/ddlhelper"
	"github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/sirupsen/logrus"
)

type Visitor struct {
}

// Enter is called before children nodes are visited.
// The returned node must be the same type as the input node n.
// skipChildren returns true means children nodes should be skipped,
// this is useful when work is done in Enter and there is no need to visit children.
func (v *Visitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	fmt.Printf("entere node: %#v\n", n)
	return n, true
}

// Leave is called after children nodes have been visited.
// The returned node's type can be different from the input node if it is a ExprNode,
// Non-expression node must be the same type as the input node n.
// ok returns false to stop visiting.
func (v *Visitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	fmt.Printf("leave node: %#v\n", n)
	return node, true
}

// concatenate files and filter by sql and print on the standard output

// catql -f "a b c d" "select a b c from file where a=b"
// catql -f "[a] b c d" "select a b c from file where a=b"
// catql -f "a|b|c|d" "select a b c from file where a=b"
// catsql -F "xx${a}yy${b}zz" "select a b c from file where a=b"

func parse(sql string) (ast.Node, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return stmtNodes[0], nil
}

type stream chan map[string]interface{}

type Option func(*streamExec)

type streamExec struct {
	stmt   *ast.SelectStmt
	output chan map[string]interface{}
	cur    map[string]interface{}

	//schema expression.Schema
	schema    *expression.Schema
	cols      []*model.ColumnInfo
	sctx      *mock.Context
	fieldType []*types.FieldType
	names     types.NameSlice
	where     expression.Expression

	Errors []error
}

func NewStreamExec(sql string, options ...Option) *streamExec {
	se := &streamExec{}
	astNode, err := parse(sql)
	if err != nil {
		logrus.Errorf("parse error: %v\n", err.Error())
		return se
	}
	se.stmt = astNode.(*ast.SelectStmt)
	se.init()
	return se
}

func (se *streamExec) HasError() bool {
	return len(se.Errors) > 0
}

func (se *streamExec) Run() error {
	go se.doSelectStmt(se.stmt)
	return nil
}

func (se *streamExec) Read() stream {
	return se.output
}

func (se *streamExec) addError(err error) {
	se.Errors = append(se.Errors, err)
}

func (se *streamExec) eval(exp ast.ExprNode) *types.Datum {
	expr, err := expression.RewriteAstExpr(se.sctx, exp, se.schema, se.names, true)
	if err != nil {
		panic(err)
	}
	data, err := expr.Eval(chunk.Row{})
	if err != nil {
		panic(err)
	}
	return &data
}

func (se *streamExec) doWhere(v map[string]interface{}) bool {
	if se.where == nil {
		return true
	}
	input := chunk.New(se.fieldType, 1024, 1)
	for i, c := range se.cols {
		dt := types.NewDatum(v[c.Name.String()])

		//fmt.Printf("%v %v %v \n", dt.ToString(), dt.Kind(), fieldType[i].GetType())
		input.AppendDatum(i, &dt)
	}
	data, err := se.where.Eval(input.GetRow(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "eval error:%v %v", v, err.Error())
		return false
	}
	res := data.GetInt64()
	if res > 0 {
		return true
	}
	return false

}
func (se *streamExec) getDataSourceByResultSet(source ast.ResultSetNode) IDatasource {
	switch src := source.(type) {
	case *ast.TableSource:
		return se.getDataSourceByResultSet(src.Source) //Datasources.Get(from.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String())
	case *ast.TableName:
		return Datasources.Get(src.Name.String())
	case *ast.SelectStmt:
		nse := &streamExec{}
		nse.stmt = src
		nse.init()
		go nse.Run()
		return nse
	default:
		panic(fmt.Sprintf("not support %#v", src))
	}
}

// ast.ResultSetNode SelectStmt, SubqueryExpr, TableSource, TableName, Join and SetOprStmt.
func (se *streamExec) getDataSource(from *ast.TableRefsClause) IDatasource {

	return se.getDataSourceByResultSet(from.TableRefs.Left)
}

func (se *streamExec) doSelectStmt(stmt *ast.SelectStmt) {
	defer close(se.output)
	if se.HasError() {
		return
	}
	count := 0
	limit := 0
	offset := 0
	if stmt.Limit != nil {
		if stmt.Limit.Count != nil {
			limit = int(se.eval(stmt.Limit.Count).GetInt64())
		}
		if stmt.Limit.Offset != nil {
			offset = int(se.eval(stmt.Limit.Offset).GetInt64())
		}
		//stmt.Limit.Count.(ast.ValueExpr).GetValue().(uint64)
	}
	ds := se.getDataSource(stmt.From)

	for v := range ds.Read() {
		if limit != 0 && count-offset >= limit {
			break
		}
		if !se.doWhere(v) {
			continue
		}
		count++
		if count <= offset {
			continue
		}
		se.output <- v
	}
}

func (se *streamExec) init() {
	se.output = make(chan map[string]interface{})
	sctx := mock.NewContext()
	createTable := "create table stdin ("
	for i := 1; i < 10; i++ {
		if i > 1 {
			createTable += ","
		}
		createTable += fmt.Sprintf("c%d varchar(1024)", i)
	}
	createTable += ")"
	createStmt, err := parse(createTable)
	if err != nil {
		panic(err)
	}
	tblInfo, err := ddlhelper.BuildTableInfoFromAST(createStmt.(*ast.CreateTableStmt))
	if err != nil {
		panic(err)
	}

	columns, names, err := expression.ColumnInfos2ColumnsAndNames(sctx, model.NewCIStr("t"), tblInfo.Name, tblInfo.Cols(), tblInfo)
	if err != nil {
		panic(err)
	}
	cols := tblInfo.Cols()
	schema := expression.NewSchema(columns...)
	sctx.GetSessionVars().CurrentDB = "t"
	fieldType := []*types.FieldType{}
	for _, v := range cols {
		fieldType = append(fieldType, v.FieldType.Clone())
	}

	se.sctx = sctx
	se.cols = cols
	se.schema = schema
	se.fieldType = fieldType
	se.names = names

	se.where = se.initWhere(se.stmt)

}

func (se *streamExec) initWhere(stmt *ast.SelectStmt) expression.Expression {
	if stmt.Where == nil {
		return nil
	}
	expr, err := expression.RewriteAstExpr(se.sctx, stmt.Where, se.schema, se.names, true)
	if err != nil {
		panic(err)
	}
	return expr
}

type IDatasource interface {
	Read() stream
}

type datasourceFactory struct {
}

func (d *datasourceFactory) Create(name string, argv []string) IDatasource {
	return &Stdin{}
}

func (d *datasourceFactory) Get(name string) IDatasource {
	if name != "stdin" {
		panic("not support table " + name)
	}
	return &Stdin{}
}

var Datasources = datasourceFactory{}

type Stdin struct {
}

func (s *Stdin) Read() stream {
	input := make(stream, 10)
	scan := bufio.NewScanner(os.Stdin)

	go func() {
		for scan.Scan() {
			val := strings.Fields(scan.Text())
			newval := val
			logrus.Debugf("val: %v %#v\n", scan.Text(), newval)
			row := map[string]interface{}{}
			for i, v := range newval {
				row[fmt.Sprintf("c%d", i+1)] = v
			}
			input <- row
		}
		close(input)
	}()
	return input
}

func main() {
	sql := flag.String("e", "", "sql")
	where := flag.String("w", "", "where ")
	datasourceName := flag.String("ds", "stdin", "datasource")
	flag.Parse()

	if *where != "" {
		*sql = fmt.Sprintf("select * from %s where %s", *datasourceName, *where)
	}
	Datasources.Create(*datasourceName, os.Args)

	//output := make(stream, 10)
	se := NewStreamExec(*sql)
	go se.Run() //.Run(stream(file), stream(os.Stdout))

	for v := range se.Read() {
		for i := 0; i < 10; i++ {
			cn := fmt.Sprintf("c%d", i+1)
			if _, ok := v[cn]; !ok {
				continue
			}
			fmt.Printf("%v ", v[cn])
		}
		fmt.Println()
	}
}
