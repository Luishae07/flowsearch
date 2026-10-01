package sgindex

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mysearch/sgpack"
)

const docWidth = 40 // every line of docs.jsonl, like [1234567,1234,187,35], is exactly this many bytes

// Doc is one page in docs.jsonl.
type Doc struct {
	ID   uint32
	Off  int64  // where the page's line starts in the pages file
	N    int32  // its length there
	DL   int    // weighted word count
	Host string // from hosts.jsonl
	hid  uint32
}

type Hit struct {
	ID    uint32
	Score float64
}

type Recent struct {
	Title string `json:"title"`
	Host  string `json:"host"`
	URL   string `json:"url"`
}

type HostCount struct {
	Host  string `json:"host"`
	Pages int    `json:"pages"`
}

type pageRec struct {
	URL         string `json:"u"`
	Title       string `json:"t"`
	Description string `json:"d"`
	Text        string `json:"x"`
	Z           string `json:"z"` // the text, packed (see sgpack)
}

// FullText returns the page text whether it was stored packed or plain.
func (r pageRec) FullText() string {
	if r.Z != "" {
		if t, err := sgpack.UnpackText(r.Z); err == nil {
			return t
		}
	}
	return r.Text
}

type rawRec struct {
	off int64
	n   int32
	rec pageRec
}

type Index struct {
	dir, pagesPath string
	mu             sync.RWMutex // segs, nDocs, dls, totalDL
	segs           []*segFile
	docsF          *os.File
	hostList       []string // host names by number; hosts.jsonl has one JSON string per line
	hostIdx        map[string]uint32
	hostsF         *os.File
	pagesF         *os.File
	nDocs          uint32
	dls            []uint16 // 2 bytes per page: its weighted length, for ranking
	totalDL        uint64

	readPos int64 // how far into pages.jsonl we have read

	hmu    sync.RWMutex
	hosts  map[string]int
	recent []Recent

	onDocs    func([]Doc)
	mergeKick chan struct{}
	Notify    chan struct{} // gets a value after every batch of new pages
}

// ---------------------------------------------------------------- open and recover

func Open(dir, pagesPath string) (*Index, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ix := &Index{dir: dir, pagesPath: pagesPath, hosts: map[string]int{}, hostIdx: map[string]uint32{}, mergeKick: make(chan struct{}, 1), Notify: make(chan struct{}, 1)}
	if hf, err := os.OpenFile(filepath.Join(dir, "hosts.jsonl"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644); err == nil {
		ix.hostsF = hf
		hr := bufio.NewReader(hf)
		for {
			l, err := hr.ReadBytes('\n')
			var h string
			if len(l) > 1 && json.Unmarshal(l, &h) == nil {
				ix.hostIdx[h] = uint32(len(ix.hostList))
				ix.hostList = append(ix.hostList, h)
			}
			if err != nil {
				break
			}
		}
	} else {
		return nil, err
	}
	docsPath := filepath.Join(dir, "docs.jsonl")
	f, err := os.OpenFile(docsPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	ix.docsF = f
	if ix.pagesF, err = os.Open(pagesPath); err != nil {
		return nil, err
	}
	st, _ := f.Stat()
	fileDocs := uint32(st.Size() / docWidth)

	// leftovers of unfinished writes
	ents, _ := os.ReadDir(dir)
	type cand struct{ base, count uint32 }
	var cands []cand
	for _, e := range ents {
		n := e.Name()
		if strings.HasPrefix(n, "tmp-seg-") || strings.HasSuffix(n, ".tmp") {
			os.Remove(filepath.Join(dir, n))
			continue
		}
		if b, c, ok := parseSegName(n); ok {
			cands = append(cands, cand{b, c})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].base != cands[j].base {
			return cands[i].base < cands[j].base
		}
		return cands[i].count > cands[j].count // the bigger one first: a merged segment beats its sources
	})
	var expected uint32
	drop := func(c cand) {
		p := filepath.Join(dir, segBase(c.base, c.count))
		os.Remove(p + ".jsonl")
		os.Remove(p + ".idx.jsonl")
	}
	for _, c := range cands {
		switch {
		case c.base+c.count > fileDocs: // written but its pages never reached docs.jsonl
			drop(c)
		case c.base == expected:
			s, err := openSeg(dir, c.base, c.count)
			if err != nil {
				drop(c)
				continue
			}
			ix.segs = append(ix.segs, s)
			expected += c.count
		default: // already covered by a merged segment, or there is a gap
			drop(c)
		}
	}
	ix.nDocs = expected
	if err := f.Truncate(int64(expected) * docWidth); err != nil {
		return nil, err
	}
	// where to carry on reading pages.jsonl
	if expected > 0 {
		d, err := ix.readDoc(expected - 1)
		if err != nil {
			return nil, err
		}
		ix.readPos = d.Off + int64(d.N)
	}
	if err := ix.scanDocs(); err != nil {
		return nil, err
	}
	log.Printf("sgindex: %d pages in %d segments, continuing pages.jsonl at byte %d", ix.nDocs, len(ix.segs), ix.readPos)
	return ix, nil
}

// hostID gives a host its number, adding it to hosts.jsonl the first time.
func (ix *Index) hostID(h string) uint32 {
	if len(h) > 200 {
		h = h[:200]
	}
	ix.hmu.Lock()
	defer ix.hmu.Unlock()
	if id, ok := ix.hostIdx[h]; ok {
		return id
	}
	id := uint32(len(ix.hostList))
	ix.hostList = append(ix.hostList, h)
	ix.hostIdx[h] = id
	b, _ := json.Marshal(h)
	ix.hostsF.Write(append(b, '\n'))
	return id
}

func (ix *Index) hostName(id uint32) string {
	ix.hmu.RLock()
	defer ix.hmu.RUnlock()
	if int(id) < len(ix.hostList) {
		return ix.hostList[id]
	}
	return ""
}

// parseDoc reads a line like [1234567,1234,187,35]: where the page is, its length, its weight, its host number.
func (ix *Index) parseDoc(b []byte, id uint32) (Doc, bool) {
	f := strings.Split(strings.Trim(strings.TrimSpace(string(b)), "[]"), ",")
	if len(f) != 4 {
		return Doc{}, false
	}
	off, e1 := strconv.ParseInt(f[0], 10, 64)
	n, e2 := strconv.Atoi(f[1])
	dl, e3 := strconv.Atoi(f[2])
	h, e4 := strconv.Atoi(f[3])
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return Doc{}, false
	}
	d := Doc{ID: id, Off: off, N: int32(n), DL: dl, hid: uint32(h)}
	d.Host = ix.hostName(d.hid)
	return d, true
}

// scanDocs reads docs.jsonl once to get page lengths and site counts, and the newest pages' records.
func (ix *Index) scanDocs() error {
	ix.dls = make([]uint16, 0, ix.nDocs+1024)
	r := bufio.NewReaderSize(io.NewSectionReader(ix.docsF, 0, int64(ix.nDocs)*docWidth), 4<<20)
	buf := make([]byte, docWidth)
	var lastDocs []Doc
	for i := uint32(0); i < ix.nDocs; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		d, ok := ix.parseDoc(buf, i)
		if !ok {
			return errors.New("bad line in docs.jsonl")
		}
		ix.dls = append(ix.dls, uint16(d.DL))
		ix.totalDL += uint64(d.DL)
		ix.hosts[d.Host]++
		lastDocs = append(lastDocs, d)
		if len(lastDocs) > 8 {
			lastDocs = lastDocs[1:]
		}
	}
	for _, d := range lastDocs {
		buf := make([]byte, d.N)
		var rec pageRec
		if _, err := ix.pagesF.ReadAt(buf, d.Off); err == nil && json.Unmarshal(buf, &rec) == nil {
			ix.recent = append(ix.recent, Recent{short(rec.Title, 90), d.Host, rec.URL})
		}
	}
	return nil
}

func short(s string, n int) string {
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

func (ix *Index) readDoc(id uint32) (Doc, error) {
	buf := make([]byte, docWidth)
	if _, err := ix.docsF.ReadAt(buf, int64(id)*docWidth); err != nil {
		return Doc{}, err
	}
	d, ok := ix.parseDoc(buf, id)
	if !ok {
		return Doc{}, errors.New("bad doc line")
	}
	return d, nil
}

// Doc returns one page's record.
func (ix *Index) Doc(id uint32) (Doc, bool) {
	d, err := ix.readDoc(id)
	return d, err == nil
}

// ---------------------------------------------------------------- taking in new pages

// Run starts the background work: following pages.jsonl, and merging segments.
func (ix *Index) Run(onDocs func([]Doc)) {
	ix.onDocs = onDocs
	go ix.tailLoop()
	go ix.mergeLoop()
}

func (ix *Index) tailLoop() {
	var pending []rawRec
	var since time.Time
	tick := time.NewTicker(25 * time.Millisecond)
	for range tick.C {
		recs, err := ix.readNew(5000 - len(pending))
		if err != nil {
			log.Printf("sgindex: reading pages.jsonl: %v", err)
			continue
		}
		if len(recs) > 0 && len(pending) == 0 {
			since = time.Now()
		}
		pending = append(pending, recs...)
		if len(pending) > 0 && (len(pending) >= 2000 || time.Since(since) >= time.Second) {
			if err := ix.commit(pending); err != nil {
				log.Printf("sgindex: could not save a batch (%v), trying again", err)
				continue
			}
			pending = pending[:0]
		}
	}
}

// readNew reads complete new lines from pages.jsonl.
func (ix *Index) readNew(max int) ([]rawRec, error) {
	st, err := os.Stat(ix.pagesPath)
	if err != nil || st.Size() <= ix.readPos || max <= 0 {
		return nil, err
	}
	f, err := os.Open(ix.pagesPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(io.NewSectionReader(f, ix.readPos, st.Size()-ix.readPos), 1<<20)
	var out []rawRec
	for len(out) < max {
		line, err := r.ReadBytes('\n')
		if err != nil { // a half-written last line is picked up next time
			break
		}
		rr := rawRec{off: ix.readPos, n: int32(len(line))}
		ix.readPos += int64(len(line))
		if json.Unmarshal(line, &rr.rec) == nil && strings.TrimSpace(rr.rec.Title+rr.rec.Text) != "" {
			out = append(out, rr)
		}
	}
	return out, nil
}

func docLine(d Doc) []byte {
	dl := d.DL
	if dl > 65535 {
		dl = 65535
	}
	s := fmt.Sprintf("[%d,%d,%d,%d]", d.Off, d.N, dl, d.hid)
	if len(s) > docWidth-1 {
		return nil
	}
	out := make([]byte, docWidth)
	copy(out, s)
	for i := len(s); i < docWidth-1; i++ {
		out[i] = ' '
	}
	out[docWidth-1] = '\n'
	return out
}

type termBuf struct {
	ps []sgpack.Pair
}

func junk(t string) bool {
	if len(t) > 30 {
		return true
	}
	if len(t) > 6 {
		for i := 0; i < len(t); i++ {
			if t[i] < '0' || t[i] > '9' {
				return false
			}
		}
		return true // a long number
	}
	return false
}

// commit turns a batch of pages into one new segment.
func (ix *Index) commit(recs []rawRec) error {
	ix.mu.RLock()
	base := ix.nDocs
	ix.mu.RUnlock()

	terms := map[string]*termBuf{}
	var docs []Doc
	var lines []byte
	var dls []uint16
	var recents []Recent
	for _, r := range recs {
		id := base + uint32(len(docs))
		counts := map[string]int{}
		dl := 0
		add := func(text string, w, limit int) {
			n := 0
			for _, t := range Tokens(text) {
				if IsStop(t) || junk(t) {
					continue
				}
				counts[t] += w
				dl += w
				if n++; limit > 0 && n >= limit {
					break
				}
			}
		}
		add(r.rec.Title, 3, 0)
		add(r.rec.Description, 2, 0)
		add(r.rec.FullText(), 1, 40) // only the first 40 words of the text: titles and descriptions carry most of the meaning
		u, err := url.Parse(r.rec.URL)
		if err != nil || u.Host == "" {
			continue
		}
		d := Doc{ID: id, Off: r.off, N: r.n, DL: dl, Host: u.Host, hid: ix.hostID(u.Host)}
		line := docLine(d)
		if line == nil {
			continue
		}
		docs = append(docs, d)
		recents = append(recents, Recent{short(r.rec.Title, 90), u.Host, r.rec.URL})
		lines = append(lines, line...)
		if dl > 65535 {
			dl = 65535
		}
		dls = append(dls, uint16(dl))
		for t, c := range counts {
			tb := terms[t]
			if tb == nil {
				tb = &termBuf{}
				terms[t] = tb
			}
			if c > 65535 {
				c = 65535
			}
			tb.ps = append(tb.ps, sgpack.Pair{Doc: id, TF: uint16(c)})
		}
	}
	if len(docs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(terms))
	for t := range terms {
		keys = append(keys, t)
	}
	sort.Strings(keys)
	w, err := newSegWriter(ix.dir, base, uint32(len(docs)))
	if err != nil {
		return err
	}
	for _, t := range keys {
		if err := w.add(t, len(terms[t].ps), []byte(sgpack.PackPostings(terms[t].ps))); err != nil {
			w.abort()
			return err
		}
	}
	seg, err := w.finish()
	if err != nil {
		return err
	}
	// the segment is on disk; now the pages' records, then both become visible together
	if _, err := ix.docsF.WriteAt(lines, int64(base)*docWidth); err != nil {
		return err
	}
	ix.docsF.Sync()
	ix.mu.Lock()
	ix.segs = append(ix.segs, seg)
	ix.nDocs += uint32(len(docs))
	ix.dls = append(ix.dls, dls...)
	for _, d := range dls {
		ix.totalDL += uint64(d)
	}
	ix.mu.Unlock()

	ix.hmu.Lock()
	for _, d := range docs {
		ix.hosts[d.Host]++
	}
	ix.recent = append(ix.recent, recents...)
	if len(ix.recent) > 8 {
		ix.recent = append([]Recent(nil), ix.recent[len(ix.recent)-8:]...)
	}
	ix.hmu.Unlock()
	select {
	case ix.Notify <- struct{}{}:
	default:
	}
	select {
	case ix.mergeKick <- struct{}{}:
	default:
	}
	if ix.onDocs != nil {
		ix.onDocs(docs)
	}
	return nil
}

// ---------------------------------------------------------------- merging

func level(count uint32) int {
	switch {
	case count < 1000:
		return 0
	case count < 8000:
		return 1
	case count < 64000:
		return 2
	case count < 512000:
		return 3
	case count < 4096000:
		return 4
	}
	return 5
}

func (ix *Index) mergeLoop() {
	tick := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ix.mergeKick:
		case <-tick.C:
		}
		for ix.mergeOnce() {
		}
	}
}

// mergeOnce joins 8 neighbouring segments of the same size class. It reports whether it did something.
const mergeFan = 4 // segments joined at a time

func (ix *Index) mergeOnce() bool {
	ix.mu.RLock()
	var run []*segFile
	for i := 0; i+mergeFan <= len(ix.segs); i++ {
		lv := level(ix.segs[i].count)
		same := true
		for j := 1; j < mergeFan; j++ {
			if level(ix.segs[i+j].count) != lv {
				same = false
				break
			}
		}
		if same {
			run = append([]*segFile(nil), ix.segs[i:i+mergeFan]...)
			break
		}
	}
	ix.mu.RUnlock()
	if run == nil {
		return false
	}
	merged, err := mergeSegs(ix.dir, run)
	if err != nil {
		log.Printf("sgindex: merge failed: %v", err)
		time.Sleep(10 * time.Second)
		return false
	}
	ix.mu.Lock()
	pos := -1
	for i, s := range ix.segs {
		if s == run[0] {
			pos = i
			break
		}
	}
	if pos < 0 || pos+mergeFan > len(ix.segs) || ix.segs[pos+mergeFan-1] != run[mergeFan-1] {
		ix.mu.Unlock() // the list changed under us; throw the merge away
		merged.close()
		os.Remove(merged.path)
		os.Remove(merged.idxPath())
		return false
	}
	out := append([]*segFile(nil), ix.segs[:pos]...)
	out = append(out, merged)
	out = append(out, ix.segs[pos+mergeFan:]...)
	ix.segs = out
	ix.mu.Unlock() // waits for searches that were running on the old files
	for _, s := range run {
		s.close()
		os.Remove(s.path)
		os.Remove(s.idxPath())
	}
	return true
}

// ---------------------------------------------------------------- statistics

func (ix *Index) NDocs() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return int(ix.nDocs)
}

func (ix *Index) Segments() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.segs)
}

// Terms is approximate: words are counted once per segment.
func (ix *Index) Terms() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	n := 0
	for _, s := range ix.segs {
		n += s.nLines
	}
	return n
}

func (ix *Index) Sites() int {
	ix.hmu.RLock()
	defer ix.hmu.RUnlock()
	return len(ix.hosts)
}

func (ix *Index) RecentList() []Recent {
	ix.hmu.RLock()
	defer ix.hmu.RUnlock()
	return append([]Recent(nil), ix.recent...)
}

func (ix *Index) TopHosts(k int) []HostCount {
	ix.hmu.RLock()
	defer ix.hmu.RUnlock()
	top := make([]HostCount, 0, len(ix.hosts))
	for h, c := range ix.hosts {
		top = append(top, HostCount{h, c})
	}
	sort.Slice(top, func(a, b int) bool { return top[a].Pages > top[b].Pages })
	if len(top) > k {
		top = top[:k]
	}
	return top
}

// ---------------------------------------------------------------- searching

type segPost struct {
	df   int
	post []byte
}

type variant struct {
	term  string
	posts []segPost
	df    int
}

type ent struct {
	doc uint32
	s   float64
}

type entHeap []ent

func (h entHeap) Len() int            { return len(h) }
func (h entHeap) Less(i, j int) bool  { return h[i].s < h[j].s }
func (h entHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *entHeap) Push(x interface{}) { *h = append(*h, x.(ent)) }
func (h *entHeap) Pop() interface{} {
	o := *h
	x := o[len(o)-1]
	*h = o[:len(o)-1]
	return x
}

// fetch reads one word from every segment. Caller holds ix.mu.
func (ix *Index) fetch(term string) variant {
	v := variant{term: term}
	for _, s := range ix.segs {
		if df, p, ok := s.lookup(term); ok {
			v.posts = append(v.posts, segPost{df, p})
			v.df += df
		}
	}
	return v
}

// prefixVariants finds the commonest words that start with prefix. Caller holds ix.mu.
func (ix *Index) prefixVariants(prefix string) []variant {
	tot := map[string]int{}
	for _, s := range ix.segs {
		for _, t := range s.prefix(prefix, 100) {
			tot[t.Term] += t.DF
		}
	}
	type td struct {
		t string
		d int
	}
	var l []td
	for t, d := range tot {
		l = append(l, td{t, d})
	}
	sort.Slice(l, func(i, j int) bool { return l[i].d > l[j].d })
	if len(l) > 8 {
		l = l[:8]
	}
	var out []variant
	for _, x := range l {
		out = append(out, ix.fetch(x.t))
	}
	return out
}

// Search ranks pages for a query. The words may be followed by site:example.com.
// It returns the word groups it used (for highlighting and title boosts) and up to 300 hits, best first.
func (ix *Index) Search(q string) (groups [][]string, hits []Hit, total int) {
	site := ""
	var words []string
	for _, f := range strings.Fields(q) {
		if strings.HasPrefix(strings.ToLower(f), "site:") {
			site = strings.TrimPrefix(strings.ToLower(f[5:]), "www.")
		} else {
			words = append(words, f)
		}
	}
	var qt []string
	for _, t := range Tokens(strings.Join(words, " ")) {
		if !IsStop(t) {
			qt = append(qt, t)
		}
	}
	if len(qt) == 0 {
		qt = Tokens(strings.Join(words, " "))
	}
	okSite := func(h string) bool {
		h = strings.TrimPrefix(h, "www.")
		return site == "" || h == site || strings.HasSuffix(h, "."+site)
	}

	ix.mu.RLock()
	defer ix.mu.RUnlock()
	N := float64(ix.nDocs)
	if N == 0 {
		return
	}
	if len(qt) == 0 {
		if site != "" {
			h := ix.scanHost(okSite, 100)
			return nil, h, len(h)
		}
		return
	}
	avg := float64(ix.totalDL) / N
	if avg < 1 {
		avg = 1
	}

	seen := map[string]bool{}
	var gv [][]variant
	for _, t := range qt {
		if seen[t] {
			continue
		}
		seen[t] = true
		v := ix.fetch(t)
		vs := []variant{v}
		if v.df < 20 && len(t) >= 3 { // a rare word: also take the words that start with it
			if pv := ix.prefixVariants(t); len(pv) > 0 {
				vs = pv
			}
		}
		if len(vs) == 1 && vs[0].df == 0 {
			continue // a word nobody has: ignore it
		}
		gv = append(gv, vs)
	}
	if len(gv) == 0 {
		return
	}
	lists := make([][]ent, len(gv))
	for gi, vs := range gv {
		var g []string
		for _, v := range vs {
			g = append(g, v.term)
		}
		groups = append(groups, g)
		var out []ent
		for _, v := range vs {
			idf := math.Log(1 + (N-float64(v.df)+0.5)/(float64(v.df)+0.5))
			for _, sp := range v.posts {
				parsePostings(sp.post, func(doc uint32, tf uint16) {
					if int(doc) >= len(ix.dls) {
						return
					}
					t := float64(tf)
					out = append(out, ent{doc, idf * t * 2.2 / (t + 1.2*(0.25+0.75*float64(ix.dls[doc])/avg))})
				})
			}
		}
		if len(vs) > 1 { // several spellings: sort by page and add up
			sort.Slice(out, func(i, j int) bool { return out[i].doc < out[j].doc })
			merged := out[:0]
			for _, e := range out {
				if n := len(merged); n > 0 && merged[n-1].doc == e.doc {
					merged[n-1].s += e.s
				} else {
					merged = append(merged, e)
				}
			}
			out = merged
		}
		lists[gi] = out
	}
	sort.Slice(lists, func(i, j int) bool { return len(lists[i]) < len(lists[j]) })
	cand := lists[0]
	for _, l := range lists[1:] {
		cand = intersect(cand, l)
	}
	if len(cand) < 10 && len(lists) > 1 { // too few pages have every word: rank pages with most of them
		type acc struct {
			s float64
			m int
		}
		all := map[uint32]*acc{}
		for _, l := range lists {
			if len(l) > 2_000_000 {
				continue
			}
			for _, e := range l {
				a := all[e.doc]
				if a == nil {
					a = &acc{}
					all[e.doc] = a
				}
				a.s += e.s
				a.m++
			}
		}
		cand = cand[:0]
		for d, a := range all {
			f := float64(a.m) / float64(len(lists))
			cand = append(cand, ent{d, a.s * f * f})
		}
	}
	total = len(cand)
	// best 2000, then the site filter, then the best 300
	keep := 2000
	if site == "" {
		keep = 300
	}
	h := &entHeap{}
	for _, e := range cand {
		if h.Len() < keep {
			heap.Push(h, e)
		} else if e.s > (*h)[0].s {
			(*h)[0] = e
			heap.Fix(h, 0)
		}
	}
	top := []ent(*h)
	sort.Slice(top, func(i, j int) bool { return top[i].s > top[j].s })
	for _, e := range top {
		if site != "" {
			d, err := ix.readDoc(e.doc)
			if err != nil || !okSite(d.Host) {
				continue
			}
		}
		hits = append(hits, Hit{e.doc, e.s})
		if len(hits) >= 300 {
			break
		}
	}
	if site != "" {
		total = len(hits)
	}
	return groups, hits, total
}

// scanHost lists pages of one site when the query has no words. Caller holds ix.mu.
func (ix *Index) scanHost(ok func(string) bool, limit int) []Hit {
	var hits []Hit
	r := bufio.NewReaderSize(io.NewSectionReader(ix.docsF, 0, int64(ix.nDocs)*docWidth), 4<<20)
	buf := make([]byte, docWidth)
	for i := uint32(0); i < ix.nDocs && len(hits) < limit; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			break
		}
		if d, good := ix.parseDoc(buf, i); good && ok(d.Host) {
			hits = append(hits, Hit{i, 1})
		}
	}
	return hits
}

func intersect(a, b []ent) []ent {
	var out []ent
	if len(b) > 16*len(a) { // a is small: look each of its pages up in b
		for _, x := range a {
			i := sort.Search(len(b), func(i int) bool { return b[i].doc >= x.doc })
			if i < len(b) && b[i].doc == x.doc {
				out = append(out, ent{x.doc, x.s + b[i].s})
			}
		}
		return out
	}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].doc < b[j].doc:
			i++
		case a[i].doc > b[j].doc:
			j++
		default:
			out = append(out, ent{a[i].doc, a[i].s + b[j].s})
			i++
			j++
		}
	}
	return out
}

// Suggest completes the last word of a query: the commonest words that start with prefix.
func (ix *Index) Suggest(prefix string, limit int) []string {
	if len(prefix) < 2 {
		return nil
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	tot := map[string]int{}
	for _, s := range ix.segs {
		for _, t := range s.prefix(prefix, 60) {
			if !junk(t.Term) && !IsStop(t.Term) {
				tot[t.Term] += t.DF
			}
		}
	}
	type td struct {
		t string
		d int
	}
	var l []td
	for t, d := range tot {
		if d >= 3 {
			l = append(l, td{t, d})
		}
	}
	sort.Slice(l, func(i, j int) bool { return l[i].d > l[j].d })
	var out []string
	for _, x := range l {
		out = append(out, x.t)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// SpellFix suggests a better spelling for a word that almost nobody has: the commonest word one edit away.
func (ix *Index) SpellFix(word string) string {
	if len(word) < 4 || len(word) > 20 {
		return ""
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	// only the big (merged) segments hold most pages; the small fresh ones are skipped to keep this quick
	var segs []*segFile
	for _, s := range ix.segs {
		if s.count >= 8000 {
			segs = append(segs, s)
		}
	}
	if len(segs) == 0 {
		segs = ix.segs
	}
	dfOf := func(t string) int {
		n := 0
		for _, s := range segs {
			n += s.dfOf(t)
		}
		return n
	}
	base := dfOf(word)
	if base >= 3 {
		return ""
	}
	const letters = "abcdefghijklmnopqrstuvwxyz"
	seen := map[string]bool{word: true}
	best, bestDF := "", base*5+10
	try := func(c string) {
		if seen[c] {
			return
		}
		seen[c] = true
		if d := dfOf(c); d > bestDF {
			best, bestDF = c, d
		}
	}
	for i := 0; i <= len(word); i++ {
		if i < len(word) {
			try(word[:i] + word[i+1:]) // one letter missing
			if i+1 < len(word) {
				try(word[:i] + string(word[i+1]) + string(word[i]) + word[i+2:]) // two letters swapped
			}
			for _, c := range letters {
				try(word[:i] + string(c) + word[i+1:]) // one letter wrong
			}
		}
		for _, c := range letters {
			try(word[:i] + string(c) + word[i:]) // one letter extra
		}
	}
	return best
}

// HostPages is how many pages we hold from one site.
func (ix *Index) HostPages(host string) int {
	ix.hmu.RLock()
	defer ix.hmu.RUnlock()
	return ix.hosts[host]
}

// HostsWithSuffix counts the addresses (like example.com and www.example.com) of a site and the pages we hold from them.
func (ix *Index) HostsWithSuffix(domain string) (hosts, pages int) {
	ix.hmu.RLock()
	defer ix.hmu.RUnlock()
	for h, n := range ix.hosts {
		if h == domain || strings.HasSuffix(h, "."+domain) {
			hosts++
			pages += n
		}
	}
	return
}

// HostsWhere counts the sites for which keep(host) is true, their pages, and the k biggest of them.
func (ix *Index) HostsWhere(keep func(string) bool, k int) (hosts, pages int, top []HostCount) {
	ix.hmu.RLock()
	defer ix.hmu.RUnlock()
	for h, n := range ix.hosts {
		if keep(h) {
			hosts++
			pages += n
			top = append(top, HostCount{h, n})
		}
	}
	sort.Slice(top, func(a, b int) bool { return top[a].Pages > top[b].Pages })
	if len(top) > k {
		top = top[:k]
	}
	return
}

// HostPagesAny is the number of pages of one site (with or without www.).
func (ix *Index) HostPagesAny(host string) int {
	ix.hmu.RLock()
	defer ix.hmu.RUnlock()
	return ix.hosts[host] + ix.hosts["www."+host]
}
