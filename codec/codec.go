package codec

import (
	"encoding/binary"

	"google.golang.org/protobuf/proto"
)

type CodecInterface interface {
	Encode(message proto.Message) ([]byte, error)
	Decode(data []byte, message proto.Message) error
}

type DefaultCodec struct {
	isLiggerEndian bool
}

const HeaderLen = 4

func NewDefaultCodec(isLiggerEndian bool) *DefaultCodec {
	return &DefaultCodec{
		isLiggerEndian: isLiggerEndian,
	}
}

func (c *DefaultCodec) Encode(message proto.Message) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	len := len(data)
	header := make([]byte, 0, HeaderLen+len)
	if c.isLiggerEndian {
		binary.LittleEndian.AppendUint32(header, uint32(len))
	} else {
		binary.BigEndian.AppendUint32(header, uint32(len))
	}
	return append(header, data...), nil
}

func (c *DefaultCodec) Decode(data []byte, message proto.Message) error {
	header := data[:HeaderLen]
	if c.isLiggerEndian {
		binary.LittleEndian.Uint32(header)
	} else {
		binary.BigEndian.Uint32(header)
	}
	return proto.Unmarshal(data[HeaderLen:], message)
}
