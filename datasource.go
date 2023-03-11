package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

type IDatasource interface {
	Read() stream
}

type datasourceFactory struct {
	mu sync.Mutex
	ds map[string]IDatasource
}

func (d *datasourceFactory) Register(name string, ds IDatasource) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ds[name] = ds
}

func (d *datasourceFactory) Get(name string) IDatasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	ds, ok := d.ds[name]
	if !ok {
		return nil
	}
	// 暂时，同个数据源，只允许使用一次
	delete(d.ds, name)
	return ds
}

var Datasources = datasourceFactory{ds: map[string]IDatasource{}}

type ChanDatasource struct {
	Output stream
}

func (s *ChanDatasource) Read() stream {
	return s.Output
}

type StdinDatasource struct {
}

func (s *StdinDatasource) Read() stream {
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
