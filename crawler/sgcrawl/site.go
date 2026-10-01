package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mysearch/sgpack"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var crawlClient = &http.Client{
	Transport: &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 8 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		MaxIdleConns:          4000,
		MaxIdleConnsPerHost:   4,
		MaxConnsPerHost:       6,
		IdleConnTimeout:       20 * time.Second,
		ForceAttemptHTTP2:     true,
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("too many redirects")
		}
		return nil
	},
}

type response struct {
	url    string // the address after redirects
	status int
	ctype  string
	body   []byte
}

type httpError struct {
	code       int
	retryAfter string
}

func (e *httpError) Error() string { return "HTTP " + strconv.Itoa(e.code) }

func textual(ct string) bool {
	ct = strings.ToLower(ct)
	return ct == "" || strings.Contains(ct, "text/") || strings.Contains(ct, "xml") || strings.Contains(ct, "json") || strings.Contains(ct, "markdown")
}

// fetch gets one address. With onlyText it does not download anything that is not a page or text.
func fetch(ctx context.Context, rawurl string, limit int64, onlyText bool, timeout time.Duration) (*response, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", rawurl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept-Language", "en")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.5")
	resp, err := crawlClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	r := &response{url: resp.Request.URL.String(), status: resp.StatusCode, ctype: resp.Header.Get("Content-Type")}
	if resp.StatusCode >= 400 {
		io.CopyN(io.Discard, resp.Body, 2000)
		return r, &httpError{resp.StatusCode, resp.Header.Get("Retry-After")}
	}
	if onlyText && (!textual(r.ctype) || resp.ContentLength > 5_000_000) {
		return r, nil
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil && len(b) == 0 {
		return r, err
	}
	if len(b) > 2 && b[0] == 0x1f && b[1] == 0x8b {
		if zr, err := gzip.NewReader(bytes.NewReader(b)); err == nil {
			if d, err := io.ReadAll(io.LimitReader(zr, 20_000_000)); err == nil {
				b = d
			}
		}
	}
	r.body = b
	return r, nil
}

var llmsRe = regexp.MustCompile(`\]\((https?://[^)\s]+)\)`)
var locRe = regexp.MustCompile(`<loc>\s*([^<\s]+)\s*</loc>`)

// discover finds the pages of a site: llms.txt, else the sitemap, else a MediaWiki API, else the front page.
func discover(ctx context.Context, site string, rb *robots, extra string) (method string, urls []string, wiki *wikiInfo) {
	if extra == "" { // a sitemap the owner sent in goes first and replaces the llms.txt step
		if r, err := fetch(ctx, site+"/llms.txt", 3_000_000, true, 15*time.Second); err == nil && len(r.body) > 0 && !bytes.HasPrefix(bytes.TrimSpace(r.body), []byte("<")) {
			for _, m := range llmsRe.FindAllSubmatch(r.body, -1) {
				urls = append(urls, string(m[1]))
			}
			if len(urls) > 0 {
				return "llms.txt", urls, nil
			}
		}
	}
	var todo []string
	if extra != "" {
		todo = []string{extra}
	} else if rb != nil && len(rb.sitemaps) > 0 {
		todo = rb.sitemaps
	} else {
		todo = []string{site + "/sitemap.xml"}
	}
	if len(todo) > 5 {
		todo = todo[:5]
	}
	for fetched := 0; len(todo) > 0 && len(urls) < 50_000 && fetched < 200 && ctx.Err() == nil; fetched++ {
		sm := todo[0]
		todo = todo[1:]
		r, err := fetch(ctx, sm, 3_000_000, true, 20*time.Second)
		if err != nil || len(r.body) == 0 {
			continue
		}
		locs := locRe.FindAllSubmatch(r.body, -1)
		index := bytes.Contains(r.body, []byte("<sitemapindex"))
		for _, m := range locs {
			if index {
				todo = append(todo, string(m[1]))
			} else {
				urls = append(urls, string(m[1]))
			}
		}
	}
	if len(urls) > 0 {
		return "sitemap", urls, nil
	}
	if w := mediawikiInfo(ctx, site); w != nil {
		return "mediawiki", nil, w
	}
	return "html", []string{site + "/"}, nil
}

// crawlSite crawls one site until it has no pages left (or the time is up). It reports whether it finished.
func crawlSite(ctx context.Context, site, extra string) bool {
	su, err := url.Parse(site)
	if err != nil {
		return true
	}
	host := su.Host
	same := func(h string) bool { return h == host || h == "www."+host || "www."+h == host }
	if _, err := fetch(ctx, site+"/", 20000, true, 10*time.Second); err != nil {
		stats.skipped.Add(1)
		return ctx.Err() == nil // a site that does not answer is skipped for a week
	}
	var rb *robots
	if r, err := fetch(ctx, site+"/robots.txt", 500_000, true, 10*time.Second); err == nil && len(r.body) > 0 {
		rb = parseRobots(string(r.body), ua)
	}
	method, queue, wiki := discover(ctx, site, rb, extra)
	if method == "html" && wiki == nil && lemmyOK(ctx, site) {
		stats.sitesStarted.Add(1)
		lemmyCrawl(ctx, site)
		return ctx.Err() == nil
	}
	if wiki != nil && !(germanOnly && !sgpack.GermanURL(site+"/")) {
		wikiCrawl(ctx, site, wiki)
		return ctx.Err() == nil
	}
	if germanOnly && !sgpack.GermanURL(site+"/") {
		// a .com site: only its German part (/de/, /de-de/ ...), from the sitemap or from /de/
		if wiki != nil {
			return true
		}
		var de []string
		for _, u := range queue {
			if sgpack.GermanURL(u) {
				de = append(de, u)
			}
		}
		if len(de) == 0 {
			method, de = "html", []string{site + "/de/"}
		}
		queue = de
	}
	stats.sitesStarted.Add(1)
	taken := 0
	seen := make(map[string]struct{}, len(queue))
	for _, u := range queue {
		seen[u] = struct{}{}
	}
	for head := 0; head < len(queue) && ctx.Err() == nil; head++ {
		u := queue[head]
		queue[head] = ""
		if head > 100000 && head > len(queue)/2 { // give the memory back
			queue = append([]string(nil), queue[head+1:]...)
			head = -1
			continue
		}
		skip := wasSent(u)
		if skip && method != "html" {
			continue
		}
		pu, err := url.Parse(u)
		if err != nil || !same(pu.Host) || !rb.allowed(pu.RequestURI()) {
			continue
		}
		r, err := fetch(ctx, u, 1_500_000, true, 20*time.Second)
		if err != nil {
			stats.errors.Add(1)
			continue
		}
		fu, err := url.Parse(r.url)
		if err != nil || !same(fu.Host) || len(r.body) == 0 {
			continue
		}
		var pg outPage
		ct := strings.ToLower(r.ctype)
		switch {
		case strings.Contains(ct, "html") || ct == "":
			p := parse(r.body, method == "html", 8000)
			pg = outPage{URL: r.url, Title: p.title, Description: p.desc, Text: p.text, Source: method}
			if method == "html" && len(seen) < 300000 {
				for _, l := range p.links {
					lu, err := fu.Parse(l)
					if err != nil || (lu.Scheme != "http" && lu.Scheme != "https") {
						continue
					}
					lu.Fragment = ""
					s := lu.String()
					if germanOnly && !sgpack.GermanURL(s) {
						continue
					}
					if _, ok := seen[s]; !ok {
						seen[s] = struct{}{}
						queue = append(queue, s)
					}
				}
			}
		case strings.Contains(ct, "text") || strings.Contains(ct, "markdown"):
			first := ""
			for _, line := range strings.Split(string(r.body[:min(len(r.body), 4000)]), "\n") {
				if t := strings.TrimSpace(strings.TrimLeft(line, "# ")); t != "" {
					first = t
					break
				}
			}
			pg = outPage{URL: r.url, Title: clip(first, 200), Text: clip(collapse(string(r.body[:min(len(r.body), 12000)])), 2000), Source: method}
		default:
			continue
		}
		if skip {
			pause(ctx)
			continue
		}
		if strings.TrimSpace(pg.Title+pg.Text) == "" {
			continue
		}
		if !langOK(pg.URL, pg.Title, pg.Description, pg.Text) {
			stats.foreign.Add(1)
			continue
		}
		if !emit(ctx, pg) {
			return false
		}
		taken++
		if maxPages > 0 && taken >= maxPages {
			break
		}
		pause(ctx)
	}
	return ctx.Err() == nil
}

func pause(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

// ---------------------------------------------------------------- MediaWiki sites (Wikipedia and friends)

type wikiInfo struct {
	api, articlePath string
}

func mediawikiInfo(ctx context.Context, site string) *wikiInfo {
	for _, path := range []string{"/w/api.php", "/api.php"} {
		r, err := fetch(ctx, site+path+"?action=query&meta=siteinfo&siprop=general&format=json", 1_000_000, true, 10*time.Second)
		if err != nil || len(r.body) == 0 {
			continue
		}
		var si struct {
			Query struct {
				General struct {
					MainPage    string `json:"mainpage"`
					ArticlePath string `json:"articlepath"`
				} `json:"general"`
			} `json:"query"`
		}
		if json.Unmarshal(r.body, &si) != nil || si.Query.General.ArticlePath == "" {
			continue
		}
		r2, err := fetch(ctx, site+path+"?action=query&prop=extracts&exintro=1&explaintext=1&format=json&formatversion=2&titles="+url.QueryEscape(si.Query.General.MainPage), 1_000_000, true, 10*time.Second)
		if err != nil || !bytes.Contains(r2.body, []byte(`"extract"`)) {
			continue
		}
		return &wikiInfo{site + path, si.Query.General.ArticlePath}
	}
	return nil
}

func waitTime(h string, def time.Duration) time.Duration {
	if n, err := strconv.Atoi(strings.TrimSpace(h)); err == nil && n > 0 {
		if n > 600 {
			n = 600
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		if d := time.Until(t); d > 0 && d < 10*time.Minute {
			return d
		}
	}
	return def
}

var wikiPathReplacer = strings.NewReplacer("%2F", "/", "%3A", ":", "%28", "(", "%29", ")", "%21", "!", "%2C", ",", "%27", "'", "%2A", "*", "%3B", ";", "%40", "@", "%24", "$", "%7E", "~")

// wikiCrawl lists every article through the wiki's own API and keeps the clean intro and short description.
// It waits when the wiki says Retry-After (HTTP 429/503) or maxlag, and asks for one thing at a time.
func wikiCrawl(ctx context.Context, site string, w *wikiInfo) {
	stats.sitesStarted.Add(1)
	call := func(params url.Values, out any) bool {
		params.Set("format", "json")
		params.Set("formatversion", "2")
		params.Set("maxlag", "5")
		for attempt := 0; attempt < 6 && ctx.Err() == nil; attempt++ {
			r, err := fetch(ctx, w.api+"?"+params.Encode(), 5_000_000, false, 30*time.Second)
			if err != nil {
				var he *httpError
				if errors.As(err, &he) && (he.code == 429 || he.code == 503) {
					sleepCtx(ctx, waitTime(he.retryAfter, 30*time.Second))
					continue
				}
				sleepCtx(ctx, time.Duration(attempt+1)*10*time.Second)
				continue
			}
			var e struct {
				Error *struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			json.Unmarshal(r.body, &e)
			if e.Error != nil && e.Error.Code == "maxlag" {
				sleepCtx(ctx, 5*time.Second)
				continue
			}
			return json.Unmarshal(r.body, out) == nil
		}
		return false
	}
	base := strings.TrimRight(site, "/")
	urlOf := func(title string) string {
		esc := wikiPathReplacer.Replace(url.PathEscape(strings.ReplaceAll(title, " ", "_")))
		return base + strings.Replace(w.articlePath, "$1", esc, 1)
	}
	cont := url.Values{}
	for ctx.Err() == nil {
		params := url.Values{"action": {"query"}, "list": {"allpages"}, "apnamespace": {"0"}, "apfilterredir": {"nonredirects"}, "aplimit": {"500"}}
		for k, v := range cont {
			params[k] = v
		}
		var all struct {
			Continue map[string]string `json:"continue"`
			Query    struct {
				Allpages []struct {
					Title string `json:"title"`
				} `json:"allpages"`
			} `json:"query"`
		}
		if !call(params, &all) {
			return
		}
		var titles []string
		for _, p := range all.Query.Allpages {
			titles = append(titles, p.Title)
		}
		for i := 0; i < len(titles) && ctx.Err() == nil; i += 20 {
			var chunk []string
			for _, t := range titles[i:min(i+20, len(titles))] {
				if !wasSent(urlOf(t)) {
					chunk = append(chunk, t)
				}
			}
			if len(chunk) == 0 {
				continue
			}
			var ex struct {
				Query struct {
					Pages []struct {
						Title       string `json:"title"`
						Extract     string `json:"extract"`
						Description string `json:"description"`
					} `json:"pages"`
				} `json:"query"`
			}
			if !call(url.Values{"action": {"query"}, "prop": {"extracts|description"}, "exintro": {"1"}, "explaintext": {"1"}, "exlimit": {"20"}, "titles": {strings.Join(chunk, "|")}}, &ex) {
				continue
			}
			for _, pg := range ex.Query.Pages {
				text := clip(collapse(pg.Extract), 2000)
				if text == "" {
					continue
				}
				if !emit(ctx, outPage{URL: urlOf(pg.Title), Title: clip(pg.Title, 200), Description: clip(pg.Description, 300), Text: text, Source: "mediawiki"}) {
					return
				}
				pause(ctx)
			}
		}
		if len(all.Continue) == 0 {
			return
		}
		cont = url.Values{}
		for k, v := range all.Continue {
			cont.Set(k, v)
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

var _ = fmt.Sprint
