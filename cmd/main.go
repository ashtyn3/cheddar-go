package main

import (
	cheddar "cheddar/pkg"
	"fmt"
)

func main() {
	inc := new(cheddar.Instance).New("db")

	t := new(cheddar.Table).New(inc.Pool, "jenna", 1)
	c := new(cheddar.Column).New(inc.Pool, "jeter", cheddar.INT64)
	t.Column(c)

	// inc.InsertTable(t)
	fmt.Println(inc.GetColumn("jenna", "jeter"))
	inc.Trace()
}
