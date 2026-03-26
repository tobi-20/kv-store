package main

func minString(a, b []string) [][]string {
	if a[0] < b[0] {
		return [][]string{a, b}
	}
	if a[0] > b[0] {
		return [][]string{b, a}
	}
	return [][]string{a, b}
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	if a > b {
		return b
	}
	return b
}
