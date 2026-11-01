//go:build !cgo

package main

import "fmt"

func listHostAccounts(string) ([]authEntry, error) {
	return nil, fmt.Errorf("native host callback unavailable")
}
