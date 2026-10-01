// Package sgpack holds SG225's own compact encodings. Everything it produces is plain text that can sit
// inside a JSON string, so the files stay JSON lines.
//
//   - base-85: binary to text with 25% overhead, using an alphabet that needs no JSON escaping
//   - packed text: deflate with a dictionary trained on our own crawl, then base-85
//   - packed postings: the gaps between page numbers and the counts as varints, then base-85
package sgpack

import (
	"bytes"
	"compress/flate"
	_ "embed"
	"encoding/binary"
	"errors"
	"io"
)

// 85 characters, none of which JSON escapes: no quote, backslash, <, > or &.
const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%()*+-./;=?@^_~[]{}|"

var dec [256]int8

func init() {
	for i := range dec {
		dec[i] = -1
	}
	for i := 0; i < len(alphabet); i++ {
		dec[alphabet[i]] = int8(i)
	}
}

var errBad = errors.New("sgpack: bad encoding")

// Encode turns bytes into text: every 4 bytes become 5 characters.
func Encode(b []byte) string {
	out := make([]byte, 0, len(b)*5/4+5)
	group := func(chunk []byte, keep int) {
		var v uint32
		for i := 0; i < 4; i++ {
			v <<= 8
			if i < len(chunk) {
				v |= uint32(chunk[i])
			}
		}
		var c [5]byte
		for i := 4; i >= 0; i-- {
			c[i] = alphabet[v%85]
			v /= 85
		}
		out = append(out, c[:keep]...)
	}
	for len(b) >= 4 {
		group(b[:4], 5)
		b = b[4:]
	}
	if len(b) > 0 {
		group(b, len(b)+1)
	}
	return string(out)
}

// Decode is the reverse of Encode.
func Decode(s string) ([]byte, error) {
	out := make([]byte, 0, len(s)*4/5+4)
	for len(s) > 0 {
		n := 5
		if len(s) < 5 {
			n = len(s)
		}
		if n == 1 {
			return nil, errBad
		}
		var v uint64
		for i := 0; i < 5; i++ {
			d := 84
			if i < n {
				x := dec[s[i]]
				if x < 0 {
					return nil, errBad
				}
				d = int(x)
			}
			v = v*85 + uint64(d)
		}
		if v > 0xFFFFFFFF {
			return nil, errBad
		}
		w := [4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
		out = append(out, w[:n-1]...)
		s = s[n:]
	}
	return out, nil
}

// ---- text ----

//go:embed sg225.dict
var dict []byte

// PackText compresses page text with the SG225 dictionary.
func PackText(s string) string {
	var buf bytes.Buffer
	w, _ := flate.NewWriterDict(&buf, flate.BestCompression, dict)
	w.Write([]byte(s))
	w.Close()
	return Encode(buf.Bytes())
}

// UnpackText reverses PackText.
func UnpackText(z string) (string, error) {
	b, err := Decode(z)
	if err != nil {
		return "", err
	}
	out, err := io.ReadAll(flate.NewReaderDict(bytes.NewReader(b), dict))
	return string(out), err
}

// ---- postings ----

// Pair is one entry of a word's list: a page number and how often (weighted) the word occurs there.
type Pair struct {
	Doc uint32
	TF  uint16
}

// PackPostings stores pages in ascending order as gaps, so most entries take 2 or 3 bytes.
func PackPostings(ps []Pair) string {
	b := make([]byte, 0, len(ps)*3)
	var prev uint32
	for _, p := range ps {
		b = binary.AppendUvarint(b, uint64(p.Doc-prev))
		b = binary.AppendUvarint(b, uint64(p.TF))
		prev = p.Doc
	}
	return Encode(b)
}

// UnpackPostings calls yield for every entry.
func UnpackPostings(s string, yield func(doc uint32, tf uint16)) error {
	b, err := Decode(s)
	if err != nil {
		return err
	}
	var doc uint64
	for len(b) > 0 {
		gap, n := binary.Uvarint(b)
		if n <= 0 {
			return errBad
		}
		b = b[n:]
		tf, n := binary.Uvarint(b)
		if n <= 0 {
			return errBad
		}
		b = b[n:]
		doc += gap
		yield(uint32(doc), uint16(tf))
	}
	return nil
}

// AppendPostings decodes a packed list onto dst.
func AppendPostings(dst []Pair, s string) []Pair {
	UnpackPostings(s, func(d uint32, tf uint16) { dst = append(dst, Pair{d, tf}) })
	return dst
}
