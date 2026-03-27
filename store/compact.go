package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func (s *Store) Compact() {
	base := 0
	a := 1
	for s.sstableCount > 1 {

		first := fmt.Sprintf("ssl_%d.txt", base)
		consec := fmt.Sprintf("ssl_%d.txt", a)

		out, err := os.Create("ssl_compact.txt")
		if err != nil {
			log.Fatal(err)
		}

		fir, err := os.Open(first)
		if err != nil {
			log.Fatal(err)
		}
		d, err := os.Open(consec)
		if err != nil {
			log.Fatal(err)
		}

		scanner1 := bufio.NewScanner(fir)
		scanner2 := bufio.NewScanner(d)

		hasLine1 := scanner1.Scan()
		hasLine2 := scanner2.Scan()

		var line1, line2 string
		var split1, split2 []string

		if hasLine1 {
			line1 = scanner1.Text()
			split1 = strings.Split(line1, ",")
		}
		if hasLine2 {
			line2 = scanner2.Text()
			split2 = strings.Split(line2, ",")
		}

		//mergesort
		for hasLine1 && hasLine2 {

			if split1[0] < split2[0] {
				fmt.Fprintln(out, line1)
				hasLine1 = scanner1.Scan() //advance and return boolean
				if hasLine1 {
					line1 = scanner1.Text() //returns most recent scanned line
					split1 = strings.Split(line1, ",")
					if len(split1) != 2 {
						log.Fatal("invalid sstable line format")
					}

				}
			} else if split1[0] > split2[0] {
				fmt.Fprintln(out, line2)
				hasLine2 = scanner2.Scan()
				if hasLine2 {
					line2 = scanner2.Text()
					split2 = strings.Split(line2, ",")
					if len(split2) != 2 {
						log.Fatal("invalid sstable line format")
					}
				}
			} else {
				fmt.Fprintln(out, line2) // newer file wins
				hasLine1 = scanner1.Scan()
				hasLine2 = scanner2.Scan()
				if hasLine1 {
					line1 = scanner1.Text()
					split1 = strings.Split(line1, ",")
				}

				if hasLine2 {
					line2 = scanner2.Text()
					split2 = strings.Split(line2, ",")
				}
			}

		}

		for hasLine1 {
			fmt.Fprintln(out, line1)
			hasLine1 = scanner1.Scan()
			if hasLine1 {
				line1 = scanner1.Text()

			}
		}
		for hasLine2 {
			fmt.Fprintln(out, line2)
			hasLine2 = scanner2.Scan()
			if hasLine2 {
				line2 = scanner2.Text()

			}
		}

		//"The merging process is complete, we switch read requests to using the new merged segment instead of the old segments — and then the old segment files can simply be deleted."
		fir.Close()
		d.Close()
		out.Close()
		err = os.Remove(first)
		if err != nil {
			log.Fatal("file failed to delete")
		}
		err = os.Remove(consec)
		if err != nil {
			log.Fatal("file failed to delete")
		}
		err = os.Rename("ssl_compact.txt", "ssl_0.txt")
		if err != nil {
			log.Fatal("file failed to be renamed")
		}
		s.sstableCount--
		a++
	}

}
