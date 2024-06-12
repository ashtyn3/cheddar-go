package cheddar

type Table struct {
	Cols  []Column
	Map   map[string]uint16
	name  CString
	keyed bool
}
