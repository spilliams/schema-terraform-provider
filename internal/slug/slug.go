// Package slug provides helper functions that generate random suffix "slugs".
package slug

import (
	"fmt"
	"math/rand"
)

const letters = "abcdefghijklmnopqrstuvwxyz"

// not terribly fast, but only used when generating new IDs.
// also not cryptographically secure, but we don't need that.
func randSeq(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return string(b)
}

func Generate(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, randSeq(10))
}
