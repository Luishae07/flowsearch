// sgcrawl is Flowsearch's crawler, written in Go so it uses every core and thousands of connections.
//
//	sgcrawl [-hours 2] [-workers 600] [-delay 50ms]
//
// It works through the seed lists (seeds_curated.txt, then cc_seeds.txt), crawls each site until it has no pages
// left (llms.txt, else the sitemap, else a MediaWiki API, else by following links), obeys robots.txt, and sends
// every page to the home server. It remembers what it sent (sent_urls.txt) and which sites are done
// (done_seeds.tsv), so it can be stopped and started at any time. Run it in its own folder.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mysearch/sgpack"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const ua = "MySearchBot/0.1 (hobby search engine; obeys robots.txt)"
const tunnelFile = "https://raw.githubusercontent.com/Luishae07/flowsearch/main/tunnel-url.txt"

// the English crawl also keeps pages addressed as German (/de/ ...), so German pages on .com sites are not lost
var germanOnly bool

var langOK = func(u, t, d, x string) bool { return sgpack.IsEnglish(u, t, d, x) || sgpack.IsGerman(u, t, d, x) }

var (
	delay       = 50 * time.Millisecond
	maxPages    int  // pages taken from one site (0 = all)
	fixedIngest bool // the ingest address was given, so do not look for a tunnel
	recrawl     = 7 * 24 * time.Hour
	pageCh      = make(chan outPage, 4000)
	token       string
	ingestURL   atomic.Value // string
	ingestHTTP  = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 64, IdleConnTimeout: 60 * time.Second}}

	sentMu   sync.RWMutex
	sentSet  = map[string]struct{}{}
	sentFile *bufio.Writer
	sentLock sync.Mutex

	stats struct {
		emitted, sent, failed, errors, skipped, sitesStarted, sitesDone, foreign atomic.Int64
	}
)

type outPage struct {
	URL         string `json:"url"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Text        string `json:"text"`
	Source      string `json:"source"`
}

func wasSent(u string) bool {
	sentMu.RLock()
	_, ok := sentSet[u]
	sentMu.RUnlock()
	return ok
}

// emit queues a page for sending. It returns false if the crawl was stopped.
func emit(ctx context.Context, p outPage) bool {
	sentMu.Lock()
	sentSet[p.URL] = struct{}{}
	sentMu.Unlock()
	stats.emitted.Add(1)
	select {
	case pageCh <- p:
		return true
	case <-ctx.Done():
		return false
	}
}

// ---------------------------------------------------------------- sending

var (
	tunnelMu   sync.Mutex
	tunnelTime time.Time
)

func refreshTunnel() {
	if fixedIngest {
		return
	}
	tunnelMu.Lock()
	defer tunnelMu.Unlock()
	if ingestURL.Load().(string) != "" && time.Since(tunnelTime) < 20*time.Second {
		return
	}
	tunnelTime = time.Now()
	resp, err := ingestHTTP.Get(tunnelFile)
	if err == nil {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1000))
		resp.Body.Close()
		if u := strings.TrimSpace(string(b)); strings.HasPrefix(u, "http") {
			ingestURL.Store(strings.TrimRight(u, "/") + "/api/ingest")
			os.WriteFile("tunnel.cache", []byte(ingestURL.Load().(string)), 0o644)
			return
		}
	}
	log.Printf("could not read the tunnel address")
	if ingestURL.Load().(string) == "" {
		if b, err := os.ReadFile("tunnel.cache"); err == nil {
			ingestURL.Store(strings.TrimSpace(string(b)))
		}
	}
}

func post(p outPage) error {
	b, _ := json.Marshal(p)
	req, err := http.NewRequest("POST", ingestURL.Load().(string), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", ua)
	resp, err := ingestHTTP.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

var spoolLock sync.Mutex

func deliver(p outPage) {
	ok := false
	for attempt := 0; attempt < 3 && !ok; attempt++ {
		if err := post(p); err != nil {
			refreshTunnel() // the address may have changed
			time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
			continue
		}
		ok = true
	}
	if ok {
		stats.sent.Add(1)
	} else {
		stats.failed.Add(1)
		spoolLock.Lock()
		if f, err := os.OpenFile("spool.jsonl", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			b, _ := json.Marshal(p)
			f.Write(append(b, '\n'))
			f.Close()
		}
		spoolLock.Unlock()
	}
	sentLock.Lock()
	sentFile.WriteString(p.URL + "\n")
	sentLock.Unlock()
}

// replaySpool sends pages that could not be sent earlier.
func replaySpool() {
	f, err := os.Open("spool.jsonl")
	if err != nil {
		return
	}
	var left [][]byte
	r := bufio.NewReaderSize(f, 1<<20)
	n := 0
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 1 {
			var p outPage
			if json.Unmarshal(line, &p) == nil {
				if post(p) != nil {
					left = append(left, line)
				} else {
					n++
				}
			}
		}
		if err != nil {
			break
		}
	}
	f.Close()
	if len(left) == 0 {
		os.Remove("spool.jsonl")
	} else {
		os.WriteFile("spool.jsonl", bytes.Join(left, nil), 0o644)
	}
	if n > 0 {
		log.Printf("sent %d pages from the spool", n)
	}
}

// ---------------------------------------------------------------- main

func readLines(path string) []string {
	var out []string
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		if l := strings.TrimSpace(sc.Text()); l != "" && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return out
}

func main() {
	hours := flag.Float64("hours", 2, "stop crawling after this many hours (0 = never)")
	workers := flag.Int("workers", 600, "sites crawled at the same time")
	flag.DurationVar(&delay, "delay", delay, "pause after every page, per site")
	ingestFlag := flag.String("ingest", "", "send pages to this address (the home server's /api/ingest) instead of looking up the tunnel")
	tokenFile := flag.String("token-file", "", "file with the ingest token (default ~/.mysearch_token)")
	seedsFile := flag.String("seeds", "", "crawl only the sites in this file (one per line), not the built-in lists")
	watch := flag.Bool("watch", false, "with -seeds: keep running and crawl new sites as they are added to the file")
	flag.IntVar(&maxPages, "maxpages", 0, "stop after this many pages from one site (0 = no limit)")
	lang := flag.String("lang", "en", "keep pages in this language: en, or de (German domains .de .at .ch .li)")
	flag.Parse()
	if *lang == "de-site" { // a site its owner says is German: judge by the text, not the address
		langOK = sgpack.IsGermanText
	}
	if *lang == "de" {
		langOK = sgpack.IsGerman
		germanOnly = true
	}

	if os.Getenv("GITHUB_ACTIONS") == "true" {
		// a one-shot Actions run: log straight to stdout so the run's own log shows what happened,
		// instead of into a file nobody on that ephemeral runner will ever read.
		log.SetOutput(os.Stdout)
		log.SetFlags(log.Ltime)
	} else {
		if st, err := os.Stat("sgcrawl.log"); err == nil && st.Size() > 20<<20 {
			os.Remove("sgcrawl.log")
		}
		lf, err := os.OpenFile("sgcrawl.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(lf)
		log.SetFlags(log.Ldate | log.Ltime)
	}

	home, _ := os.UserHomeDir()
	tpath := home + "/.mysearch_token"
	if *tokenFile != "" {
		tpath = *tokenFile
	}
	tb, err := os.ReadFile(tpath)
	if err != nil {
		log.Fatal("no token file ", tpath)
	}
	token = strings.TrimSpace(string(tb))
	ingestURL.Store("")
	if *ingestFlag != "" {
		fixedIngest = true
		ingestURL.Store(*ingestFlag)
	} else {
		refreshTunnel()
	}

	// what was already sent
	for _, u := range readLines("sent_urls.txt") {
		sentSet[u] = struct{}{}
	}
	sf, err := os.OpenFile("sent_urls.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	sentFile = bufio.NewWriterSize(sf, 1<<16)
	go func() {
		for range time.Tick(time.Second) {
			sentLock.Lock()
			sentFile.Flush()
			sentLock.Unlock()
		}
	}()

	// which sites are done
	doneAt := map[string]int64{}
	for _, l := range readLines("done_seeds.tsv") {
		if a := strings.Split(l, "\t"); len(a) == 2 {
			if t, err := strconv.ParseFloat(a[1], 64); err == nil {
				doneAt[strings.ReplaceAll(a[0], " ", "\t")] = int64(t)
			}
		}
	}
	doneF, _ := os.OpenFile("done_seeds.tsv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	var doneLock sync.Mutex

	isDue := func(s string) bool {
		doneLock.Lock()
		t := doneAt[s]
		doneLock.Unlock()
		return time.Now().Unix()-t > int64(recrawl.Seconds())
	}
	var seeds []string
	loadSeeds := func() []string {
		var out []string
		seenSeed := map[string]bool{}
		var lists []string
		if *seedsFile != "" {
			for _, l := range readLines(*seedsFile) {
				l, sitemap, _ := strings.Cut(l, "\t") // "site<TAB>sitemap address": the owner sent a sitemap
				if !strings.Contains(l, "://") {
					l = "https://" + l
				}
				if u, err := url.Parse(l); err == nil && u.Host != "" {
					seed := u.Scheme + "://" + u.Host
					if sm, err := url.Parse(strings.TrimSpace(sitemap)); err == nil && sitemap != "" && sm.Host != "" && (sm.Scheme == "http" || sm.Scheme == "https") {
						seed += "\t" + sm.String()
					}
					lists = append(lists, seed)
				}
			}
		} else {
			curated := readLines("seeds_curated.txt")
			rand.Shuffle(len(curated), func(i, j int) { curated[i], curated[j] = curated[j], curated[i] })
			lists = append(curated, readLines("cc_seeds.txt")...)
		}
		for _, s := range lists {
			if !seenSeed[s] {
				seenSeed[s] = true
				if isDue(s) {
					out = append(out, s)
				}
			}
		}
		return out
	}
	seeds = loadSeeds()
	log.Printf("started: %d sites to crawl, %d workers, %d pages already sent", len(seeds), *workers, len(sentSet))

	ctx, cancel := context.WithCancel(context.Background())
	if *hours > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(*hours*float64(time.Hour)))
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	signal.Ignore(syscall.SIGHUP, syscall.SIGPIPE) // the SSH session that started us ending must not stop the crawl
	go func() {
		for s := range sig {
			if s == syscall.SIGINT { // the way to stop us by hand: pkill -INT sgcrawl
				log.Printf("got signal %v, stopping", s)
				cancel()
				return
			}
			// something in the Codespace keeps sending SIGTERM. Note who is around, and carry on.
			out, _ := exec.Command("ps", "-eo", "pid,ppid,etimes,args").Output()
			os.WriteFile("signal.ps", out, 0o644)
			log.Printf("got %v: ignored (process list saved in signal.ps)", s)
		}
	}()

	var senders sync.WaitGroup
	for i := 0; i < 64; i++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			for p := range pageCh {
				deliver(p)
			}
		}()
	}
	go replaySpool()

	go func() { // progress line every 5 seconds
		var last int64
		for range time.Tick(5 * time.Second) {
			e := stats.emitted.Load()
			log.Printf("%.0f pages/s | %d crawled, %d sent, %d failed | sites: %d started, %d done, %d skipped | queue %d | %d errors | %d not English",
				float64(e-last)/5, e, stats.sent.Load(), stats.failed.Load(), stats.sitesStarted.Load(), stats.sitesDone.Load(), stats.skipped.Load(), len(pageCh), stats.errors.Load(), stats.foreign.Load())
			last = e
		}
	}()

	runPool := func(list []string) {
		jobs := make(chan string)
		var crawlers sync.WaitGroup
		for i := 0; i < *workers; i++ {
			crawlers.Add(1)
			go func() {
				defer crawlers.Done()
				for job := range jobs {
					if ctx.Err() != nil {
						continue
					}
					site, extra, _ := strings.Cut(job, "\t")
					if crawlSite(ctx, site, extra) {
						stats.sitesDone.Add(1)
						doneLock.Lock()
						doneAt[job] = time.Now().Unix()
						fmt.Fprintf(doneF, "%s\t%d\n", strings.ReplaceAll(job, "\t", " "), time.Now().Unix())
						doneLock.Unlock()
					}
				}
			}()
		}
		for _, s := range list {
			select {
			case jobs <- s:
			case <-ctx.Done():
			}
			if ctx.Err() != nil {
				break
			}
		}
		close(jobs)
		crawlers.Wait()
	}
	runPool(seeds)
	for *watch && *seedsFile != "" && ctx.Err() == nil { // wait for sites to be added to the file
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Second):
		}
		if more := loadSeeds(); len(more) > 0 && ctx.Err() == nil {
			log.Printf("%d new site(s) in the list", len(more))
			runPool(more)
		}
	}
	log.Printf("crawling over, sending what is left (%d queued)", len(pageCh))
	close(pageCh)
	waitDone := make(chan struct{})
	go func() { senders.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Minute):
	}
	sentLock.Lock()
	sentFile.Flush()
	sentLock.Unlock()
	log.Printf("stopped: %d pages crawled, %d sent", stats.emitted.Load(), stats.sent.Load())
}
