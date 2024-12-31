package protocol

func MurmurHash3KeySum(key [16]byte) uint32 {
	hash := uint32(42)

	k := uint32(key[0]) | uint32(key[1])<<8 | uint32(key[2])<<16 | uint32(key[3])<<24
	k *= 0xcc9e2d51
	k = (k << 15) | (k >> 17)
	k *= 0x1b873593
	hash ^= k
	hash = (hash << 13) | (hash >> 19)
	hash = hash*5 + 0xe6546b64

	k = uint32(key[4]) | uint32(key[5])<<8 | uint32(key[6])<<16 | uint32(key[7])<<24
	k *= 0xcc9e2d51
	k = (k << 15) | (k >> 17)
	k *= 0x1b873593
	hash ^= k
	hash = (hash << 13) | (hash >> 19)
	hash = hash*5 + 0xe6546b64

	k = uint32(key[8]) | uint32(key[9])<<8 | uint32(key[10])<<16 | uint32(key[11])<<24
	k *= 0xcc9e2d51
	k = (k << 15) | (k >> 17)
	k *= 0x1b873593
	hash ^= k
	hash = (hash << 13) | (hash >> 19)
	hash = hash*5 + 0xe6546b64

	k = uint32(key[12]) | uint32(key[13])<<8 | uint32(key[14])<<16 | uint32(key[15])<<24
	k *= 0xcc9e2d51
	k = (k << 15) | (k >> 17)
	k *= 0x1b873593
	hash ^= k
	hash = (hash << 13) | (hash >> 19)
	hash = hash*5 + 0xe6546b64

	hash ^= uint32(16)
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16
	return hash
}
