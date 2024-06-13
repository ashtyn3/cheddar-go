package main

import (
	cheddar "cheddar/pkg"
	"fmt"
)

func main() {
	inc := new(cheddar.Instance).New("db")

	t := new(cheddar.Table).New(inc.Pool, "a", 1)
	c := new(cheddar.Column).New(inc.Pool, "a", cheddar.INT64)
	t.Column(c)

	// inc.InsertTable(t)
	fmt.Println(inc.GetColumn("a", "a"))
	inc.Trace()
}
