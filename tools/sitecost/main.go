// sitecost: what does a visitor cost each site's owner? For every site in crawler/sites (plus Flowsearch's
// biggest indexed sites, when its public tunnel answers) it loads the home page the way a browser would
// (the HTML and up to 25 linked images, scripts, styles and fonts), adds up the bytes sent, and turns that
// into money with a simple cloud-bandwidth price. Writes site-costs.json and SITE-COSTS.md in -root.
//
// This is bandwidth only: servers, storage and people cost extra, so the real cost is higher.
package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const ua = "flowsearch-sitecost/1.0 (+https://github.com/Luishae07/flowsearch)"

var assetRe = regexp.MustCompile(`(?i)(?:src|href)=["']([^"']+\.(?:js|css|png|jpe?g|gif|webp|avif|svg|ico|woff2?)(?:\?[^"']*)?)["']`)

var client = &http.Client{
	Timeout:   10 * time.Second,
	Transport: &http.Transport{DisableCompression: true, MaxIdleConnsPerHost: 4}, // we want the bytes as sent
}

type Site struct {
	Host          string  `json:"host"`
	PagesIndexed  int     `json:"pages_indexed,omitempty"`
	HTMLBytes     int64   `json:"html_bytes,omitempty"`
	Assets        int     `json:"assets,omitempty"`
	BytesPerVisit int64   `json:"bytes_per_visit,omitempty"`
	USDPer1000    float64 `json:"usd_per_1000_visits,omitempty"`
	USDPerHour    float64 `json:"usd_per_hour,omitempty"`
	USDPer10H     float64 `json:"usd_per_10_hours,omitempty"`
	JSBytes       int64   `json:"js_bytes,omitempty"`
	Requests      int     `json:"requests,omitempty"`
	Basis         string  `json:"basis,omitempty"` // "browser" (JavaScript ran) or "static" (HTML and linked files only)
	Error         string  `json:"error,omitempty"`
}

func get(rawURL, method string, limit int64) (*http.Response, []byte, error) {
	req, err := http.NewRequest(method, rawURL, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if method == http.MethodHead {
		return resp, nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	return resp, body, err
}

func measure(host string, egress float64, visits int) Site {
	s := Site{Host: host}
	resp, body, err := get("https://"+host+"/", http.MethodGet, 3_000_000)
	if err != nil {
		s.Error = shorten(err.Error())
		return s
	}
	s.HTMLBytes = int64(len(body))
	if resp.ContentLength > 0 && resp.ContentLength < s.HTMLBytes {
		s.HTMLBytes = resp.ContentLength
	}
	text := body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		if zr, err := gzip.NewReader(strings.NewReader(string(body))); err == nil {
			if plain, err := io.ReadAll(io.LimitReader(zr, 6_000_000)); err == nil {
				text = plain
			}
		}
	}
	seen := map[string]bool{}
	var refs []string
	base := resp.Request.URL
	for _, m := range assetRe.FindAllStringSubmatch(string(text), -1) {
		u, err := base.Parse(m[1])
		if err != nil || seen[u.String()] {
			continue
		}
		seen[u.String()] = true
		refs = append(refs, u.String())
	}
	sort.Strings(refs)
	if len(refs) > 25 {
		refs = refs[:25]
	}
	sizes := make([]int64, len(refs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 6)
	for i, r := range refs {
		wg.Add(1)
		go func(i int, r string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if hr, _, err := get(r, http.MethodHead, 0); err == nil && hr.ContentLength > 0 {
				sizes[i] = hr.ContentLength
			}
		}(i, r)
	}
	wg.Wait()
	total := s.HTMLBytes
	for _, n := range sizes {
		total += n
		if n > 0 {
			s.Assets++
		}
	}
	s.Basis = "static"
	s.setCost(total, egress, visits)
	return s
}

func (s *Site) setCost(total int64, egress float64, visits int) {
	s.BytesPerVisit = total
	perVisit := float64(total) / 1e9 * egress
	s.USDPer1000 = round(perVisit*1000, 4)
	s.USDPerHour = round(perVisit*float64(visits), 4)
	s.USDPer10H = round(perVisit*float64(visits)*10, 4)
}

func round(v float64, places int) float64 {
	p := 1.0
	for i := 0; i < places; i++ {
		p *= 10
	}
	return float64(int64(v*p+0.5)) / p
}

func shorten(s string) string {
	if len(s) > 60 {
		return s[:60]
	}
	return s
}

func envNum(name string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil && v > 0 {
		return v
	}
	return def
}

func main() {
	root := flag.String("root", ".", "repo root (holds crawler/sites; the results are written here)")
	useBrowser := flag.Bool("browser", true, "also load every page in headless Chrome so JavaScript is counted")
	flag.Parse()
	egress := envNum("EGRESS_PER_GB", 0.085)
	visits := int(envNum("VISITS_PER_HOUR", 1000))

	pages := map[string]int{}
	files, _ := filepath.Glob(filepath.Join(*root, "crawler", "sites", "*.txt"))
	for _, f := range files {
		pages[strings.TrimSuffix(filepath.Base(f), ".txt")] = 0
	}
	// more sites: the most popular ones from the crawler's Tranco seed list (TOP_SITES of them, default 100)
	if f, err := os.ReadFile(filepath.Join(*root, "crawler", "seeds-tranco.txt")); err == nil {
		want := int(envNum("TOP_SITES", 100))
		for _, line := range strings.Split(string(f), "\n") {
			if want <= 0 {
				break
			}
			if h := strings.TrimSpace(line); h != "" {
				if _, have := pages[h]; !have {
					pages[h] = 0
				}
				want--
			}
		}
	}
	// the biggest indexed sites, from the live engine (best effort)
	if resp, body, err := get("https://raw.githubusercontent.com/Luishae07/flowsearch/main/web-tunnel-url.txt", http.MethodGet, 1000); err == nil && resp.StatusCode == 200 {
		if _, sb, err := get(strings.TrimSpace(string(body))+"/api/stats", http.MethodGet, 4_000_000); err == nil {
			var st struct {
				TopSites []struct {
					Host  string `json:"host"`
					Pages int    `json:"pages"`
				} `json:"top_sites"`
			}
			if json.Unmarshal(sb, &st) == nil {
				for _, t := range st.TopSites {
					pages[t.Host] = t.Pages
				}
			}
		}
	} else {
		fmt.Println("live stats not available")
	}

	hosts := make([]string, 0, len(pages))
	for h := range pages {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	var browser context.Context
	if *useBrowser {
		var stop context.CancelFunc
		if browser, stop = newBrowser(); browser != nil {
			defer stop()
		} else {
			fmt.Println("Chrome is not available: counting HTML and linked files only")
		}
	}
	rows := make([]Site, len(hosts))
	browserSem := make(chan struct{}, 3)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i, h := range hosts {
		wg.Add(1)
		go func(i int, h string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rows[i] = measure(h, egress, visits)
			rows[i].PagesIndexed = pages[h]
			if browser != nil {
				browserSem <- struct{}{}
				total, js, n, err := browserMeasure(browser, h)
				<-browserSem
				if err == nil && total > 0 {
					rows[i].Error = ""
					rows[i].JSBytes, rows[i].Requests, rows[i].Basis = js, n, "browser"
					rows[i].setCost(total, egress, visits)
				}
			}
		}(i, h)
	}
	wg.Wait()

	var ok, bad []Site
	for _, r := range rows {
		if r.Error == "" {
			ok = append(ok, r)
		} else {
			bad = append(bad, r)
		}
	}
	sort.Slice(ok, func(i, j int) bool { return ok[i].USDPerHour > ok[j].USDPerHour })

	out, _ := json.MarshalIndent(map[string]any{"egress_usd_per_gb": egress, "visits_per_hour": visits, "sites": ok, "unreachable": bad}, "", " ")
	os.WriteFile(filepath.Join(*root, "site-costs.json"), out, 0o644)

	var md strings.Builder
	fmt.Fprintf(&md, "# What a visitor costs each site\n\nBandwidth only, at **$%g/GB** and **%d visits per hour**. Every site's home page is loaded in a real browser with JavaScript running, counting every byte it receives (sites Chrome cannot load fall back to HTML plus up to 25 linked files, marked n/a under JavaScript). Servers, storage and people cost extra.\n\n", egress, visits)
	md.WriteString("| Site | Page weight | of which JavaScript | Requests | $ per 1,000 visits | $ per hour | $ per 10 hours |\n|---|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range ok {
		js, reqs := "n/a", r.Assets+1
		if r.Basis == "browser" {
			js, reqs = fmt.Sprintf("%d KB", r.JSBytes/1024), r.Requests
		}
		fmt.Fprintf(&md, "| %s | %d KB | %s | %d | %.4f | %.4f | %.4f |\n", r.Host, r.BytesPerVisit/1024, js, reqs, r.USDPer1000, r.USDPerHour, r.USDPer10H)
	}
	if len(bad) > 0 {
		names := make([]string, len(bad))
		for i, b := range bad {
			names[i] = b.Host
		}
		fmt.Fprintf(&md, "\nCould not load: %s\n", strings.Join(names, ", "))
	}
	os.WriteFile(filepath.Join(*root, "SITE-COSTS.md"), []byte(md.String()), 0o644)
	fmt.Printf("%d measured, %d unreachable\n", len(ok), len(bad))
	_ = url.Parse
}
