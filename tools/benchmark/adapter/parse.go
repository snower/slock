package adapter

import (
	"crypto/md5"
	"encoding/hex"
)

func parseLockKey(argId string) [16]byte {
	lockKey := [16]byte{}
	arg_len := len(argId)
	if arg_len == 16 {
		lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7],
			lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15] =
			byte(argId[0]), byte(argId[1]), byte(argId[2]), byte(argId[3]), byte(argId[4]), byte(argId[5]), byte(argId[6]),
			byte(argId[7]), byte(argId[8]), byte(argId[9]), byte(argId[10]), byte(argId[11]), byte(argId[12]), byte(argId[13]), byte(argId[14]), byte(argId[15])
	} else if arg_len > 16 {
		if arg_len == 32 {
			v, err := hex.DecodeString(argId)
			if err == nil {
				lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7],
					lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15] =
					v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
					v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
			} else {
				v := md5.Sum([]byte(argId))
				lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7],
					lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15] =
					v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
					v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
			}
		} else {
			v := md5.Sum([]byte(argId))
			lockKey[0], lockKey[1], lockKey[2], lockKey[3], lockKey[4], lockKey[5], lockKey[6], lockKey[7],
				lockKey[8], lockKey[9], lockKey[10], lockKey[11], lockKey[12], lockKey[13], lockKey[14], lockKey[15] =
				v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
				v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
		}
	} else {
		argIndex := 16 - arg_len
		for i := 0; i < 16; i++ {
			if i < argIndex {
				lockKey[i] = 0
			} else {
				lockKey[i] = argId[i-argIndex]
			}
		}
	}
	return lockKey
}
