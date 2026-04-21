package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

func (s *Store) Compact() {
	base := 0
	next := 1

	for s.sstableCount > 1 {

		first := fmt.Sprintf("ssl_%d.txt", base)
		second := fmt.Sprintf("ssl_%d.txt", next)

		out, err := os.Create("ssl_0_compacting.tmp")
		if err != nil {
			log.Fatal(err)
		}

		f1, err := os.Open(first)
		if err != nil {
			log.Fatal(err)
		}
		f2, err := os.Open(second)
		if err != nil {
			log.Fatal(err)
		}

		r1 := bufio.NewReader(f1)
		r2 := bufio.NewReader(f2)

		var (
			op1, op2 Operation
			k1, k2   []byte
			v1, v2   []byte
			ok1, ok2 bool
		)

		// helper to read one record
		read := func(r *bufio.Reader) (Operation, []byte, []byte, bool) {
			var op Operation
			var kLen, vLen uint32

			if err := binary.Read(r, binary.BigEndian, &op); err != nil {
				return 0, nil, nil, false
			}

			if err := binary.Read(r, binary.BigEndian, &kLen); err != nil {
				return 0, nil, nil, false
			}

			key := make([]byte, kLen)
			if _, err := io.ReadFull(r, key); err != nil {
				return 0, nil, nil, false
			}

			if err := binary.Read(r, binary.BigEndian, &vLen); err != nil {
				return 0, nil, nil, false
			}

			val := make([]byte, vLen)
			if _, err := io.ReadFull(r, val); err != nil {
				return 0, nil, nil, false
			}

			return op, key, val, true
		}

		op1, k1, v1, ok1 = read(r1)
		op2, k2, v2, ok2 = read(r2)

		for ok1 && ok2 {

			// compare keys
			if string(k1) < string(k2) {

				writeRecord(out, op1, k1, v1)
				op1, k1, v1, ok1 = read(r1)

			} else if string(k1) > string(k2) {

				writeRecord(out, op2, k2, v2)
				op2, k2, v2, ok2 = read(r2)

			} else {
				// newer wins
				writeRecord(out, op2, k2, v2)

				op1, k1, v1, ok1 = read(r1)
				op2, k2, v2, ok2 = read(r2)
			}
		}

		for ok1 {
			writeRecord(out, op1, k1, v1)
			op1, k1, v1, ok1 = read(r1)
		}

		for ok2 {
			writeRecord(out, op2, k2, v2)
			op2, k2, v2, ok2 = read(r2)
		}

		f1.Close()
		f2.Close()

		out.Sync()
		out.Close()

		os.Rename("ssl_0_compacting.tmp", "ssl_0.txt")
		os.Remove(first)
		os.Remove(second)

		s.sstableCount--
		next++
	}
}
