package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ---------------------------------------------------------------- Lemmy servers (open Reddit-like communities)

// lemmyOK says whether the site is a Lemmy server (its public API answers).
func lemmyOK(ctx context.Context, site string) bool {
	r, err := fetch(ctx, site+"/api/v3/site", 2_000_000, true, 10*time.Second)
	return err == nil && bytes.Contains(r.body, []byte(`"site_view"`))
}

// lemmyCrawl reads every post through the server's own API, newest first, 50 at a time. It waits when the
// server says to slow down and gives up after repeated errors.
func lemmyCrawl(ctx context.Context, site string) {
	taken, fails := 0, 0
	for page := 1; ctx.Err() == nil; page++ {
		r, err := fetch(ctx, fmt.Sprintf("%s/api/v3/post/list?type_=All&sort=New&limit=50&page=%d", site, page), 8_000_000, true, 30*time.Second)
		if err != nil {
			var he *httpError
			if errors.As(err, &he) && (he.code == 429 || he.code == 503) && fails < 5 {
				fails++
				sleepCtx(ctx, waitTime(he.retryAfter, 30*time.Second))
				page--
				continue
			}
			stats.errors.Add(1)
			return
		}
		fails = 0
		var lp struct {
			Posts []struct {
				Post struct {
					ID      int    `json:"id"`
					Name    string `json:"name"`
					Body    string `json:"body"`
					URL     string `json:"url"`
					Deleted bool   `json:"deleted"`
					Removed bool   `json:"removed"`
				} `json:"post"`
				Community struct {
					Title string `json:"title"`
					Name  string `json:"name"`
				} `json:"community"`
			} `json:"posts"`
		}
		if json.Unmarshal(r.body, &lp) != nil || len(lp.Posts) == 0 {
			return
		}
		for _, p := range lp.Posts {
			if p.Post.Deleted || p.Post.Removed || p.Post.ID == 0 {
				continue
			}
			u := fmt.Sprintf("%s/post/%d", site, p.Post.ID)
			if wasSent(u) {
				continue
			}
			text := clip(collapse(p.Post.Body), 2000)
			desc := "c/" + p.Community.Name
			if p.Community.Title != "" {
				desc += " - " + p.Community.Title
			}
			pg := outPage{URL: u, Title: clip(strings.TrimSpace(p.Post.Name), 200), Description: desc, Text: text, Source: "lemmy"}
			if strings.TrimSpace(pg.Title+pg.Text) == "" || !langOK(pg.URL, pg.Title, pg.Description, pg.Text) {
				stats.foreign.Add(1)
				continue
			}
			if !emit(ctx, pg) {
				return
			}
			taken++
			if maxPages > 0 && taken >= maxPages {
				return
			}
		}
		pause(ctx)
	}
}
