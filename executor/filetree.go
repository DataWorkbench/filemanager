package executor

import (
	"strings"

	"github.com/DataWorkbench/common/utils/idgenerator"
)

type FileTree struct {
	id    string
	name  string
	path  string
	level int32
	next  *FileTree
	pre   *FileTree
}

func NewFileTree(filePath string, generator *idgenerator.IDGenerator) (tree *FileTree, err error) {
	if strings.HasSuffix(filePath, fileSplit) {
		filePath = filePath[:len(filePath)-1]
	}
	values := strings.Split(filePath, fileSplit)
	var tmp = &FileTree{}
	var level int32 = 0
	tree = tmp
	for _, v := range values {
		level++
		var id string
		if id, err = generator.Take(); err != nil {
			return
		}
		if v == "" {
			tmp.name = fileSplit
			tmp.path = fileSplit
			tmp.id = id
			tmp.level = level
		} else {
			tmp.next = &FileTree{
				id:    id,
				name:  v,
				path:  tmp.path + v + fileSplit,
				next:  nil,
				level: level,
			}
			t := tmp
			tmp = tmp.next
			tmp.pre = t
		}
	}
	return
}

func (tree *FileTree) TravelTree(f func(tree *FileTree) error) (err error) {
	if tree == nil {
		return
	}
	if err = f(tree); err != nil {
		return
	}
	if err = tree.next.TravelTree(f); err != nil {
		return
	}
	return
}
