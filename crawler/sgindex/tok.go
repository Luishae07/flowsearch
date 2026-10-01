// Package sgindex is SG225's own search index. Everything on disk is JSON lines:
//
//	pages.jsonl     the crawled pages (written by the ingest service, never changed)
//	docs.jsonl      one line per page, padded to exactly 512 bytes, so page N is at byte N*512
//	seg-*.jsonl     inverted index segments: one line per word, sorted: {"t":"word","df":2,"p":"12:3,57:1"}
//	seg-*.idx.jsonl a small sparse index of each segment (every 256th word and its byte offset)
//
// New pages become a small segment within a second; background merges join segments.
// Nothing big is held in RAM: searches read the files (the operating system caches them).
package sgindex

import (
	"strings"
	"unicode"
)

var stop = map[string]bool{}

func init() {
	for _, w := range strings.Fields("the a an of to in and or is it for on with as by at from that this are was be not but have has had you your we our they their its which who what when where how can will would there about into than then also more most some any all one two if so do does did no yes up out over under after before just only very such other these those been being were them he she him her his hers i me my mine us ours") {
		stop[w] = true
	}
}

// IsStop reports whether a word is too common to search for.
func IsStop(w string) bool { return stop[w] }

// Tokens splits text into lower-case words (letters and digits).
func Tokens(s string) []string {
	var out []string
	var b strings.Builder
	flush := func() {
		if b.Len() > 0 {
			if t := b.String(); len(t) <= 80 {
				out = append(out, t)
			}
			b.Reset()
		}
	}
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			b.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return out
}
