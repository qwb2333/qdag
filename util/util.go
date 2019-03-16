package util

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"
)

func DeepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func GetTimeStampMs() int64 {
	return time.Now().UnixNano() / 1000000
}

func GetTimeStamp() int64 {
	return time.Now().Unix()
}

func LogInit() {
	log.SetFlags(log.Lshortfile|log.Lmicroseconds)
}