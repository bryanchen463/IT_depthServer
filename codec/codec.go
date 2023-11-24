package codec

import (
	"encoding/binary"
	"errors"
	"io"

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
		header = binary.LittleEndian.AppendUint32(header, uint32(len))
	} else {
		header = binary.BigEndian.AppendUint32(header, uint32(len))
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
	return proto.Unmarshal(data, message)
}

type Conn interface {
	io.Reader
	// Peek returns the next n bytes without advancing the reader. The bytes stop
	// being valid at the next read call. If Peek returns fewer than n bytes, it
	// also returns an error explaining why the read is short. The error is
	// ErrBufferFull if n is larger than b's buffer size.
	//
	// Note that the []byte buf returned by Peek() is not allowed to be passed to a new goroutine,
	// as this []byte will be reused within event-loop.
	// If you have to use buf in a new goroutine, then you need to make a copy of buf and pass this copy
	// to that new goroutine.
	Peek(n int) (buf []byte, err error)

	// Discard skips the next n bytes, returning the number of bytes discarded.
	//
	// If Discard skips fewer than n bytes, it also returns an error.
	// If 0 <= n <= b.Buffered(), Discard is guaranteed to succeed without
	// reading from the underlying io.Reader.
	Discard(n int) (discarded int, err error)

	// InboundBuffered returns the number of bytes that can be read from the current buffer.
	InboundBuffered() (n int)
}

const headerSize = 4

var (
	ErrNotEnoughHeader = errors.New("not enough header bytes")
	ErrInvalidData     = errors.New("invalid data")
)

func TryGetMessage(c Conn, isLittleEnd bool) (message [][]byte, err error) {
	for {
		inboundBuffered := c.InboundBuffered()
		if inboundBuffered < headerSize {
			break
		}
		headerLen, err := c.Peek(headerSize)
		if err != nil {
			return nil, err
		}
		var dataLen uint32
		if isLittleEnd {
			dataLen = binary.LittleEndian.Uint32(headerLen)
		} else {
			dataLen = binary.BigEndian.Uint32(headerLen)
		}
		peekData, err := c.Peek(int(dataLen) + headerSize)
		if err != nil {
			return nil, err
		}
		if len(peekData) != int(dataLen)+headerSize {
			break
		} else {
			c.Discard(int(dataLen) + headerSize)
		}
		message = append(message, peekData[headerSize:])
	}
	return
}
