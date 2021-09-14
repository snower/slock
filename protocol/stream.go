package protocol

type ISteam interface {
	ReadBytes(b []byte) (int, error)
	Read(b []byte) (int, error)
	WriteBytes(b []byte) error
	Write(b []byte) (int, error)
	Close() error
}