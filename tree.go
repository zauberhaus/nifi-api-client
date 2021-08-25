package nifi

import (
	"fmt"
	"io"
)

type Tree map[*Component]Tree

func (t Tree) Add(c *Component) {
	t[c] = Tree{}
}

func (t Tree) Merge(tree Tree) {
	for k, v := range tree {
		t[k] = v
	}

}

func (t Tree) Fprint(w io.Writer, root bool, padding string) {
	if t == nil {
		return
	}

	index := 0
	for k, v := range t {
		fmt.Fprintf(w, "%s%s\n", padding+getPadding(root, getBoxType(index, len(t))), k)
		v.Fprint(w, false, padding+getPadding(root, getBoxTypeExternal(index, len(t))))
		index++
	}
}

type BoxType int

const (
	Regular BoxType = iota
	Last
	AfterLast
	Between
)

func (boxType BoxType) String() string {
	switch boxType {
	case Regular:
		return "\u251c\u2500" // ├
	case Last:
		return "\u2514\u2500" // └
	case AfterLast:
		return " "
	case Between:
		return "\u2502" // │
	default:
		panic("invalid box type")
	}
}

func getBoxType(index int, len int) BoxType {
	if index+1 == len {
		return Last
	} else if index+1 > len {
		return AfterLast
	}
	return Regular
}

func getBoxTypeExternal(index int, len int) BoxType {
	if index+1 == len {
		return AfterLast
	}
	return Between
}

func getPadding(root bool, boxType BoxType) string {
	if root {
		return ""
	}

	return boxType.String() + " "
}
