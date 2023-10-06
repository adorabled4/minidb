package imitate_minidb

import "errors"

var (
	ErrKeyNotFound   = errors.New("key not found in database")
	ErrInvalidDBFile = errors.New("invalid DBfile")
)
