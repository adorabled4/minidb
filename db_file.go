package imitate_minidb

import (
	"os"
	"path/filepath"
	"sync"
)

const FileName = "minidb.data"
const MergeFileName = "minidb.data.merge"

type DBFile struct {
	File             *os.File   `json:"file" doc:"数据文件"`
	Offset           int64      `json:"offset,omitempty" doc:"空闲位置偏移量"`
	HeaderBufferPool *sync.Pool `json:"header_buffer_pool" doc:"头部缓冲池"`
}

func newInternal(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	pool := &sync.Pool{New: func() interface{} {
		return make([]byte, entryHeaderSize)
	}}

	return &DBFile{
		File:             file,
		Offset:           stat.Size(),
		HeaderBufferPool: pool,
	}, nil
}

// NewDBFile 创建一个新的数据文件
func NewDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, FileName)
	return newInternal(fileName)
}

// NewMergeDBFile 新建一个合并时的数据文件
func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, MergeFileName)
	return newInternal(fileName)
}

// Read 从offset处开始读取
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := df.HeaderBufferPool.Get().([]byte)
	defer df.HeaderBufferPool.Put(buf)
	_, err = df.File.ReadAt(buf, offset)
	if err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}
	// 越过偏移量(10byte : keySize + valueSize + Mark)读取 当前entry的 key 值
	offset += entryHeaderSize

	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)

	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}

	return

}

// Write 写入数据文件 : decode => write(byte offset)
func (df *DBFile) Write(e *Entry) (err error) {
	buf, err := e.Encode()
	if err != nil {
		return
	}
	_, err = df.File.WriteAt(buf, df.Offset)
	if err != nil {
		return
	}
	return
}
