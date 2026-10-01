// Package sgseen remembers which page addresses were already stored, without keeping them in RAM.
// The files are JSON lines of fixed width, {"h":"<16 hex digits>"}, 25 bytes each:
//
//	seen-main.jsonl  sorted, found by binary search (line N is at byte N*25)
//	seen-new.jsonl   the newest addresses in arrival order (a small set is also kept in memory)
//
// When the new file gets large it is merged into the main file.
package sgseen

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

const lineLen = 25

type Set struct {
	dir   string
	mu    sync.Mutex
	main  *os.File
	nMain int64
	fresh map[uint64]struct{}
	newF  *os.File
}

func hashOf(url string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(url))
	return h.Sum64()
}

func line(h uint64) []byte { return []byte(fmt.Sprintf(`{"h":"%016x"}`+"\n", h)) }

func parseLine(b []byte) (uint64, bool) {
	if len(b) < lineLen-1 || b[0] != '{' {
		return 0, false
	}
	v, err := strconv.ParseUint(string(b[6:22]), 16, 64)
	return v, err == nil
}

// Open loads the set. If there is no main file yet it is built from pages.jsonl (once).
func Open(dir, pagesPath string) (*Set, error) {
	s := &Set{dir: dir, fresh: map[uint64]struct{}{}}
	mainPath := filepath.Join(dir, "seen-main.jsonl")
	if _, err := os.Stat(mainPath); os.IsNotExist(err) {
		if err := s.buildFrom(pagesPath); err != nil {
			return nil, err
		}
	}
	f, err := os.Open(mainPath)
	if err != nil {
		return nil, err
	}
	st, _ := f.Stat()
	s.main, s.nMain = f, st.Size()/lineLen
	newPath := filepath.Join(dir, "seen-new.jsonl")
	if nf, err := os.Open(newPath); err == nil {
		r := bufio.NewReader(nf)
		for {
			b, err := r.ReadBytes('\n')
			if len(b) == lineLen {
				if h, ok := parseLine(b); ok {
					s.fresh[h] = struct{}{}
				}
			}
			if err != nil {
				break
			}
		}
		nf.Close()
	}
	s.newF, err = os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	return s, err
}

func (s *Set) buildFrom(pagesPath string) error {
	var hs []uint64
	if f, err := os.Open(pagesPath); err == nil {
		r := bufio.NewReaderSize(f, 1<<20)
		for {
			b, err := r.ReadBytes('\n')
			if len(b) > 0 {
				var p struct {
					URL  string `json:"url"`
					URL2 string `json:"u"`
				}
				if json.Unmarshal(b, &p) == nil {
					if p.URL == "" {
						p.URL = p.URL2
					}
					if p.URL != "" {
						hs = append(hs, hashOf(p.URL))
					}
				}
			}
			if err != nil {
				break
			}
		}
		f.Close()
	}
	return s.writeMain(hs, nil)
}

// writeMain writes the sorted union of a and b (b may be nil) as the new main file.
func (s *Set) writeMain(a []uint64, b []uint64) error {
	all := append(a, b...)
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	tmp := filepath.Join(s.dir, "seen-main.jsonl.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var last uint64
	for i, h := range all {
		if i > 0 && h == last {
			continue
		}
		last = h
		w.Write(line(h))
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()
	return os.Rename(tmp, filepath.Join(s.dir, "seen-main.jsonl"))
}

func (s *Set) inMain(h uint64) bool {
	lo, hi := int64(0), s.nMain
	buf := make([]byte, lineLen)
	for lo < hi {
		mid := (lo + hi) / 2
		if _, err := s.main.ReadAt(buf, mid*lineLen); err != nil {
			return false
		}
		v, _ := parseLine(buf)
		switch {
		case v == h:
			return true
		case v < h:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return false
}

// Add records an address and reports whether it was new.
func (s *Set) Add(url string) bool {
	h := hashOf(url)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.fresh[h]; ok || s.inMain(h) {
		return false
	}
	s.fresh[h] = struct{}{}
	s.newF.Write(line(h))
	if n := int64(len(s.fresh)); n >= 100000 && n >= s.nMain/8 {
		s.merge()
	}
	return true
}

func (s *Set) merge() {
	fresh := make([]uint64, 0, len(s.fresh))
	for h := range s.fresh {
		fresh = append(fresh, h)
	}
	sort.Slice(fresh, func(i, j int) bool { return fresh[i] < fresh[j] })
	tmp := filepath.Join(s.dir, "seen-main.jsonl.tmp")
	out, err := os.Create(tmp)
	if err != nil {
		return
	}
	w := bufio.NewWriterSize(out, 1<<20)
	r := bufio.NewReaderSize(io.NewSectionReader(s.main, 0, s.nMain*lineLen), 1<<20)
	buf := make([]byte, lineLen)
	readMain := func() (uint64, bool) {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, false
		}
		return parseLine(buf)
	}
	mv, mok := readMain()
	i := 0
	for mok || i < len(fresh) {
		if !mok || (i < len(fresh) && fresh[i] < mv) {
			w.Write(line(fresh[i]))
			i++
		} else {
			w.Write(line(mv))
			mv, mok = readMain()
		}
	}
	if w.Flush() != nil {
		out.Close()
		return
	}
	out.Sync()
	out.Close()
	mainPath := filepath.Join(s.dir, "seen-main.jsonl")
	if os.Rename(tmp, mainPath) != nil {
		return
	}
	s.main.Close()
	nf, err := os.Open(mainPath)
	if err != nil {
		return
	}
	st, _ := nf.Stat()
	s.main, s.nMain = nf, st.Size()/lineLen
	s.newF.Truncate(0)
	s.fresh = map[uint64]struct{}{}
}

// Len is the number of addresses known.
func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.nMain) + len(s.fresh)
}
