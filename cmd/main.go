package main

import (
	cheddar "cheddar/pkg"

	"go.uber.org/zap"
)

func main() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction(zap.WithCaller(false))))

	inc := new(cheddar.Instance)
	inc.New("db")

	t := new(cheddar.Table).New("hi", 1)
	c := new(cheddar.Column).New(cheddar.CString("age"), cheddar.INT64)
	t.Column(c)

	inc.InsertTable(t)
	inc.Trace()

	// d, _ := inc.GetRowSegment([]byte("hi.8.cpk8pfq1jl9ckrhqf540"))

	defer zap.L().Sync()
	inc.Db.Close()
}
