package protocol

type ISteam interface {
	ReadBytes(b []byte) (int, error)
	Read(b []byte) (int, error)
	WriteBytes(b []byte) error
	Write(b []byte) (int, error)
	Close() error
}

type MemBytesArrayStream struct {
	datas [][]byte
	index int
}

func NewMemBytesArrayStream() *MemBytesArrayStream {
	return &MemBytesArrayStream{datas: make([][]byte, 0), index: 0}
}

func (self *MemBytesArrayStream) ReadBytes(b []byte) (int, error) {
	r, n := 0, 0
	for _, data := range self.datas {
		if len(b)-n >= len(data)-self.index {
			size := len(data) - self.index
			copy(b[n:], data[self.index:])
			self.index = 0
			n += size
			r++
		} else {
			size := len(b) - n
			copy(b[n:], data[self.index:self.index+size])
			self.index += size
			n += size
			break
		}
	}

	if r > 0 {
		self.datas = self.datas[r:]
	}
	return n, nil
}

func (self *MemBytesArrayStream) Read(b []byte) (int, error) {
	return self.ReadBytes(b)
}

func (self *MemBytesArrayStream) WriteBytes(b []byte) error {
	self.datas = append(self.datas, b)
	return nil
}

func (self *MemBytesArrayStream) Write(b []byte) (int, error) {
	self.datas = append(self.datas, b)
	return len(b), nil
}

func (self *MemBytesArrayStream) Close() error {
	return nil
}

func (self *MemBytesArrayStream) Size() int {
	size := 0
	for i, data := range self.datas {
		if i == 0 {
			size += len(data) - self.index
		} else {
			size += len(data)
		}
	}
	return size
}
