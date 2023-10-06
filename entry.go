package imitate_minidb

import "encoding/binary"

// headSize 10 byte
const entryHeaderSize = 10
const (
	PUT uint16 = iota
	DELETE
)

type Entry struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
}

func NewEntry(key, value []byte, mark uint16) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Mark:      mark,
	}
}

func (e *Entry) GetSize() int64 {
	return int64(entryHeaderSize + e.KeySize + e.ValueSize)
}

// 编码 Entry => 字节数组
func (e *Entry) Encode() ([]byte, error) {
	// [crc][KeySize][ValueSize][Mark][Key][Value][type]
	//  2      4        4        2${KeySize} ${ValueSize} 2  2
	//  在编码的时候不会计算crc(仅仅是Entry的内容进行encode)
	buf := make([]byte, e.GetSize())
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.BigEndian.PutUint16(buf[8:10], e.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)
	return buf, nil
}

// 解码 , 字节数组转换成 Entry
func Decode(buf []byte) (*Entry, error) {
	keySize := binary.BigEndian.Uint32(buf[0:4])
	valueSize := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	return &Entry{
		Key:       nil,
		Value:     nil,
		KeySize:   keySize,
		ValueSize: valueSize,
		Mark:      mark,
	}, nil
}
