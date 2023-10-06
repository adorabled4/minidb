package imitate_minidb

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type MiniDB struct {
	indexes map[string]int64 `info:"key:offset"`
	dbFile  *DBFile
	dirPath string
	mutex   sync.Mutex `doc:"互斥锁"`
}

// Put 插入数据 , 需要写索引以及写入数据文件
func (db *MiniDB) Put(key, value []byte) (err error) {
	if len(key) == 0 {
		return
	}
	db.mutex.Lock()
	defer db.mutex.Unlock()
	offset := db.dbFile.Offset
	// write data to file
	entry := NewEntry(key, value, PUT)
	err = db.dbFile.Write(entry)
	if err != nil {
		return err
	}
	// write index to mem
	db.indexes[string(key)] = offset
	return
}

// exist key值是否存在与数据库
// 若存在返回偏移量；不存在返回ErrKeyNotFound
func (db *MiniDB) exist(key []byte) (offset int64, err error) {
	offset, ok := db.indexes[string(key)]
	if !ok {
		return 0, ErrKeyNotFound
	}
	return offset, nil
}

// Get 通过key读取数据
func (db *MiniDB) Get(key []byte) (val []byte, err error) {
	offset, err := db.exist(key)
	if err == ErrKeyNotFound {
		return
	}
	db.mutex.Lock()
	defer db.mutex.Unlock()
	// 从磁盘中读取数据
	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil {
		return
	}
	if e != nil {
		val = e.Value
	}
	return
}

// Del 删除数据
func (db *MiniDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return nil
	}
	if _, err = db.exist(key); err != nil {
		if err == ErrKeyNotFound {
			return nil
		} else {
			return err
		}
	}
	// 封装成 Entry 并写入
	e := NewEntry(key, nil, DELETE)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}
	// 删除内存中的 key
	delete(db.indexes, string(key))

	return
}

// loadIndexesFromFile 从文件中加载索引
func (db *MiniDB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}
	var offset int64 = 0
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// read completely
			if err == io.EOF {
				break
			}
			return
		}
		db.indexes[string(e.Key)] = offset
		// 对于删除的记录需要删除存储的索引
		if e.Mark != DELETE {
			delete(db.indexes, string(e.Key))
		}
		offset += e.GetSize()
	}
	return
}

// 关闭数据库
func (db *MiniDB) Close() error {
	if db.dbFile == nil {
		return nil
	}
	return db.dbFile.File.Close()
}

func Open(path string) (db *MiniDB, err error) {
	// 如果数据库目录不存在，则新建一个
	if _, err = os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, err
		}
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// 创建数据文件
	dbFile, err := NewDBFile(absPath)
	if err != nil {
		return nil, err
	}

	// 创建数据库对象
	db = &MiniDB{
		indexes: make(map[string]int64),
		dbFile:  dbFile,
		dirPath: absPath,
	}
	// 加载索引
	db.loadIndexesFromFile()
	return
}

// Merge  合并entry , 清除无用的数据记录
func (db *MiniDB) Merge() error {
	var (
		validEntries []*Entry
		offset       int64
	)
	// 读取原数据文件中的entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return err
		}
		// 存在索引并且索引是最新的(说明数据是有效的) , 比如同一个key前后插入了多次数据
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		// make ptr move
		offset += e.GetSize()
	}
	if len(validEntries) > 0 {
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		// 最后需要删除临时文件
		defer os.Remove(mergeDBFile.File.Name())
		db.mutex.Lock()
		defer db.mutex.Unlock()
		// 把新的有效的entry写入到 文件中
		for _, e := range validEntries {
			newOffset := mergeDBFile.Offset
			err := mergeDBFile.Write(e)
			if err != nil {
				return err
			}
			// 更新索引
			db.indexes[string(e.Key)] = newOffset
		}
		// 获取文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)
		mergeDBFile.File.Close()
		// 获取文件名
		mergeDBFileName := mergeDBFile.File.Name()
		// 临时文件变更为新的数据文件
		os.Rename(mergeDBFileName, filepath.Join(db.dirPath, FileName))
		dbFile, err := NewDBFile(db.dirPath)
		if err != nil {
			return err
		}
		db.dbFile = dbFile
	}
	return nil
}
