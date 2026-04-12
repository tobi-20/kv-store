package main

import (
	"bytes"
	"encoding/binary"
	"io"
)

func encoder(key, value string) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, uint32(len(key))) //writes the binary representation of the length of key into buf so the decoder knows how many bytes it should read next
	if err != nil {
		return nil, err
	}
	_, err = buf.WriteString(key)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.BigEndian, uint32(len(value)))
	if err != nil {
		return nil, err
	}
	_, err = buf.WriteString(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err

}

func decoder(msg []byte) (string, string, error) {
	var keyLen, valueLen uint32

	r := bytes.NewReader(msg)
	err := binary.Read(r, binary.BigEndian, &keyLen)
	if err != nil {
		return "", "", err
	}
	key := make([]byte, keyLen)
	_, err = io.ReadFull(r, key) //reads exactly len(key) bytes from r into key
	if err != nil {
		return "", "", err
	}
	err = binary.Read(r, binary.BigEndian, &valueLen)
	if err != nil {
		return "", "", err
	}
	value := make([]byte, valueLen)
	_, err = io.ReadFull(r, value)
	if err != nil {
		return "", "", err
	}

	return string(key), string(value), nil
}
