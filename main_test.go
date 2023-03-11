package main

import (
	"fmt"
	"net/url"
	"testing"

	_ "github.com/pingcap/tidb/planner/core"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func Test_simpleSelect(t *testing.T) {
	se := NewStreamExec(`SELECT  c1, c2 FROM t where c1>10 and c1 <20 `)
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
	Datasources.Register("t", &ChanDatasource{Output: input})
	//output := make(chan map[string]interface{})

	go se.Run()

	count := 0
	for v := range se.Read() {
		count++
		t.Logf("res:%v\n", v)
	}
	t.Logf("%v %d \n", se.Errors, count)
	if count != 9 {
		t.Fatalf("count error:%d", count)
	}
}

func Test_DatasourceUrl(t *testing.T) {
	tests := []struct {
		url string
		t   string
	}{
		{"file:///tmp/test.txt", "file"},
		{"stdin://", "stdin"},
	}
	for _, v := range tests {
		u, err := url.Parse(v.url)
		if err != nil {
			t.Errorf("parse url error:%v", err)
			continue
		}
		if u.Scheme != v.t {
			t.Errorf("parse url error:%v", err)
			continue
		}

	}
}
