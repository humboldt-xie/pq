package main

import (
	"fmt"
	"testing"

	_ "github.com/pingcap/tidb/planner/core"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func newFunction() {
	se := NewStreamExec(`SELECT  c1, c2 FROM t where c1>10 and c1 <20 group by c1`)
	if se.HasError() {
		fmt.Printf("parse error: %v\n", se.Errors)
		return
	}
	input := make(chan map[string]interface{})
	go func() {
		for i := 0; i < 1000; i++ {
			input <- map[string]interface{}{
				"c1": fmt.Sprintf("%d", i),
				"c2": "x",
			}
		}
		close(input)
	}()
	output := make(chan map[string]interface{})

	go se.Run(input, output)

	for v := range output {
		fmt.Printf("res:%v\n", v)
	}
}

func Test_newFunction(t *testing.T) {
	newFunction()
	t.Fatalf("not implemented")
	tests := []struct {
		name string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newFunction()
		})
	}
}

func Test_DatasourceUrl(t *testing.T) {
	tests := []struct {
		name string
	}{}
}
