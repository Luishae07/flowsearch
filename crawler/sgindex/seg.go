package sgindex

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"mysearch/sgpack"
)

const blockLines = 256 // every 256th word goes into the sparse index

// segFile is one index segment: a JSON-lines file sorted by word, plus its small sparse index.
type segFile struct {
	path        string
	f           *os.File
	size        int64
	base, count uint32 // it covers documents base .. base+count-1
	idxTerms    []string
	idxOffs     []int64
	nLines      int // approximate: blocks * 256
}

func segBase(base, count uint32) string { return fmt.Sprintf("seg-%010d-%08d", base, count) }

func parseSegName(name string) (base, count uint32, ok bool) {
	if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".jsonl") || strings.HasSuffix(name, ".idx.jsonl") {
		return
	}
	var b, c uint64
	if n, err := fmt.Sscanf(strings.TrimSuffix(name, ".jsonl"), "seg-%d-%d", &b, &c); err != nil || n != 2 {
		return
	}
	return uint32(b), uint32(c), true
}

func (s *segFile) idxPath() string { return strings.TrimSuffix(s.path, ".jsonl") + ".idx.jsonl" }

func openSeg(dir string, base, count uint32) (*segFile, error) {
	p := filepath.Join(dir, segBase(base, count)+".jsonl")
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &segFile{path: p, f: f, size: st.Size(), base: base, count: count}
	if err := s.loadIdx(); err != nil {
		if err := s.rebuildIdx(); err != nil {
			f.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *segFile) close() { s.f.Close() }

func idxLineBytes(term string, off int64) []byte {
	b, _ := json.Marshal([]any{term, off})
	return append(b, '\n')
}

func (s *segFile) loadIdx() error {
	f, err := os.Open(s.idxPath())
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	s.idxTerms, s.idxOffs = nil, nil
	for sc.Scan() {
		var l []any
		if json.Unmarshal(sc.Bytes(), &l) != nil || len(l) != 2 {
			return fmt.Errorf("bad idx line")
		}
		t, _ := l[0].(string)
		o, _ := l[1].(float64)
		s.idxTerms = append(s.idxTerms, t)
		s.idxOffs = append(s.idxOffs, int64(o))
	}
	if len(s.idxTerms) == 0 && s.size > 0 {
		return fmt.Errorf("empty idx")
	}
	s.nLines = len(s.idxTerms) * blockLines
	return nil
}

// rebuildIdx rewrites the sparse index by reading the whole segment once.
func (s *segFile) rebuildIdx() error {
	r := bufio.NewReaderSize(io.NewSectionReader(s.f, 0, s.size), 1<<20)
	var off int64
	n := 0
	var out bytes.Buffer
	s.idxTerms, s.idxOffs = nil, nil
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			t, _, _, ok := splitLine(bytes.TrimRight(line, "\n"))
			if ok && n%blockLines == 0 {
				s.idxTerms = append(s.idxTerms, t)
				s.idxOffs = append(s.idxOffs, off)
				out.Write(idxLineBytes(t, off))
			}
			off += int64(len(line))
			n++
		}
		if err != nil {
			break
		}
	}
	s.nLines = n
	tmp := s.idxPath() + ".tmp"
	if err := os.WriteFile(tmp, out.Bytes(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.idxPath())
}

// splitLine parses ["word",3,"postings"] without a full JSON parse.
func splitLine(line []byte) (term string, df int, post []byte, ok bool) {
	if !bytes.HasPrefix(line, []byte(`["`)) {
		return
	}
	rest := line[2:]
	i := bytes.Index(rest, []byte(`",`))
	if i < 0 {
		return
	}
	term = string(rest[:i])
	rest = rest[i+2:]
	j := bytes.Index(rest, []byte(`,"`))
	if j < 0 {
		return
	}
	df, _ = strconv.Atoi(string(rest[:j]))
	post = rest[j+2:]
	if len(post) < 2 {
		return
	}
	return term, df, post[:len(post)-2], true // drop the closing "]
}

// parsePostings calls yield(doc, tf) for each entry of a packed list.
func parsePostings(p []byte, yield func(doc uint32, tf uint16)) {
	sgpack.UnpackPostings(string(p), yield)
}

func (s *segFile) reader(block int) *bufio.Reader {
	off := s.idxOffs[block]
	return bufio.NewReaderSize(io.NewSectionReader(s.f, off, s.size-off), 32<<10)
}

// firstBlock returns the last block whose first word is <= term.
func (s *segFile) firstBlock(term string) int {
	if len(s.idxTerms) == 0 {
		return -1
	}
	i := sort.Search(len(s.idxTerms), func(i int) bool { return s.idxTerms[i] > term }) - 1
	if i < 0 {
		i = 0
	}
	return i
}

// lookup finds one word. The postings bytes are a copy.
func (s *segFile) lookup(term string) (df int, post []byte, ok bool) {
	b := s.firstBlock(term)
	if b < 0 {
		return
	}
	r := s.reader(b)
	for n := 0; n < blockLines+1; n++ {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			t, d, p, good := splitLine(bytes.TrimRight(line, "\n"))
			if good {
				if t == term {
					return d, append([]byte(nil), p...), true
				}
				if t > term {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
	return
}

// dfOf returns how many pages have a word, without copying its list.
func (s *segFile) dfOf(term string) int {
	b := s.firstBlock(term)
	if b < 0 {
		return 0
	}
	r := s.reader(b)
	for n := 0; n < blockLines+1; n++ {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			if t, d, _, good := splitLine(bytes.TrimRight(line, "\n")); good {
				if t == term {
					return d
				}
				if t > term {
					return 0
				}
			}
		}
		if err != nil {
			return 0
		}
	}
	return 0
}

type termDF struct {
	Term string
	DF   int
}

// prefix lists up to limit words that start with prefix.
func (s *segFile) prefix(prefix string, limit int) []termDF {
	b := s.firstBlock(prefix)
	if b < 0 {
		return nil
	}
	var out []termDF
	r := s.reader(b)
	for len(out) < limit {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			t, d, _, good := splitLine(bytes.TrimRight(line, "\n"))
			if good {
				if strings.HasPrefix(t, prefix) {
					out = append(out, termDF{t, d})
				} else if t > prefix {
					break
				}
			}
		}
		if err != nil {
			break
		}
	}
	return out
}

// ---- writing a segment ----

type segWriter struct {
	dir         string
	base, count uint32
	tmp         *os.File
	w           *bufio.Writer
	off         int64
	n           int
	idx         bytes.Buffer
}

func newSegWriter(dir string, base, count uint32) (*segWriter, error) {
	tmp, err := os.CreateTemp(dir, "tmp-seg-*")
	if err != nil {
		return nil, err
	}
	return &segWriter{dir: dir, base: base, count: count, tmp: tmp, w: bufio.NewWriterSize(tmp, 1<<20)}, nil
}

func (w *segWriter) add(term string, df int, post []byte) error {
	if w.n%blockLines == 0 {
		w.idx.Write(idxLineBytes(term, w.off))
	}
	var head bytes.Buffer
	head.WriteString(`["`)
	head.WriteString(term)
	head.WriteString(`",`)
	head.WriteString(strconv.Itoa(df))
	head.WriteString(`,"`)
	if _, err := w.w.Write(head.Bytes()); err != nil {
		return err
	}
	if _, err := w.w.Write(post); err != nil {
		return err
	}
	if _, err := w.w.WriteString("\"]\n"); err != nil {
		return err
	}
	w.off += int64(head.Len() + len(post) + 3)
	w.n++
	return nil
}

func (w *segWriter) abort() {
	w.tmp.Close()
	os.Remove(w.tmp.Name())
}

// finish makes the file permanent and returns it opened.
func (w *segWriter) finish() (*segFile, error) {
	if err := w.w.Flush(); err != nil {
		w.abort()
		return nil, err
	}
	if err := w.tmp.Sync(); err != nil {
		w.abort()
		return nil, err
	}
	w.tmp.Close()
	final := filepath.Join(w.dir, segBase(w.base, w.count)+".jsonl")
	if err := os.Rename(w.tmp.Name(), final); err != nil {
		os.Remove(w.tmp.Name())
		return nil, err
	}
	idx := strings.TrimSuffix(final, ".jsonl") + ".idx.jsonl"
	if err := os.WriteFile(idx+".tmp", w.idx.Bytes(), 0o644); err != nil {
		return nil, err
	}
	if err := os.Rename(idx+".tmp", idx); err != nil {
		return nil, err
	}
	return openSeg(w.dir, w.base, w.count)
}

// ---- merging segments ----

type cursor struct {
	r    *bufio.Reader
	term string
	df   int
	post []byte
	ok   bool
}

func (c *cursor) next() {
	for {
		line, err := c.r.ReadBytes('\n')
		if len(line) > 0 {
			if t, d, p, good := splitLine(bytes.TrimRight(line, "\n")); good {
				c.term, c.df, c.post, c.ok = t, d, append(c.post[:0], p...), true
				return
			}
		}
		if err != nil {
			c.ok = false
			return
		}
	}
}

// mergeSegs joins neighbouring segments (their document ranges touch) into one. Because the document
// numbers only go up from one segment to the next, merging is just sticking the posting strings together.
func mergeSegs(dir string, in []*segFile) (*segFile, error) {
	var count uint32
	for _, s := range in {
		count += s.count
	}
	w, err := newSegWriter(dir, in[0].base, count)
	if err != nil {
		return nil, err
	}
	cs := make([]*cursor, len(in))
	for i, s := range in {
		cs[i] = &cursor{r: bufio.NewReaderSize(io.NewSectionReader(s.f, 0, s.size), 1<<20)}
		cs[i].next()
	}
	var pairs []sgpack.Pair
	for {
		min := ""
		found := false
		for _, c := range cs {
			if c.ok && (!found || c.term < min) {
				min, found = c.term, true
			}
		}
		if !found {
			break
		}
		pairs = pairs[:0]
		df := 0
		for _, c := range cs { // the segments are in page order, so the lists just follow each other
			if c.ok && c.term == min {
				pairs = sgpack.AppendPostings(pairs, string(c.post))
				df += c.df
				c.next()
			}
		}
		if err := w.add(min, df, []byte(sgpack.PackPostings(pairs))); err != nil {
			w.abort()
			return nil, err
		}
	}
	return w.finish()
}
