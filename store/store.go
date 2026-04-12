package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"os"
	"strings"
)

// load file from disk
func NewStore(path string) (*Store, error) {

	s := &Store{

		memtable: make(map[string]string),
		index:    make(map[string][]SparseIndexEntry),
		path:     path,
	}
	wal, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) //"... if the database crashes, the most recent writes (which are in the memtable but not yet written out to disk) are lost. In order to avoid that problem, we can keep a separate log on disk to which every write is immediately appended"
	if err != nil {
		return nil, err
	}

	s.wal = wal

	q, err := os.Open("wal.log")
	if err != nil {
		return nil, err
	}

	files, err := os.ReadDir(".") //reads the named directory, returning all its directory entries sorted by filename.
	if err != nil {
		log.Println(err)
	}

	for _, v := range files {
		if strings.HasPrefix(v.Name(), "ssl_") {
			s.sstableCount++
		}
	}

	//read the wal log into the memtable in case of crash or sudden restart
	reader := bufio.NewReader(q)
	var keyLen, valueLen uint32
	for {
		err := binary.Read(reader, binary.BigEndian, &keyLen)
		if err != nil {
			log.Println(err)
			break
		}
		key := make([]byte, keyLen)
		_, err = io.ReadFull(reader, key) //reads exactly len(key) bytes from r into key
		if err != nil {
			log.Println(err)
			break
		}
		err = binary.Read(reader, binary.BigEndian, &valueLen)
		if err != nil {
			log.Println(err)
			break
		}
		value := make([]byte, valueLen)
		_, err = io.ReadFull(reader, value)
		if err != nil {
			log.Println(err)
			break
		}
		s.memtable[string(key)] = string(value)
	}
	q.Close()
	return s, nil
}
