package main

import "strings"

type rule struct {
	allow bool
	pat   string
}

type robots struct {
	rules    []rule
	sitemaps []string
	noSearch bool // "Content-Signal: search=no": the site does not want to be in a search index
}

// parseRobots reads robots.txt for our crawler: the group that names us, or else the group for everyone.
func parseRobots(body, agent string) *robots {
	agent = strings.ToLower(agent)
	var ours, star []rule
	oursSig, starSig := map[string]string{}, map[string]string{}
	var sitemaps []string
	var cur []string // agents of the current group
	sawRule := false
	for _, line := range strings.Split(body, "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		i := strings.IndexByte(line, ':')
		if i < 0 {
			continue
		}
		key, val := strings.ToLower(strings.TrimSpace(line[:i])), strings.TrimSpace(line[i+1:])
		switch key {
		case "user-agent":
			if sawRule {
				cur, sawRule = nil, false
			}
			cur = append(cur, strings.ToLower(val))
		case "allow", "disallow":
			sawRule = true
			r := rule{key == "allow", val}
			for _, a := range cur {
				if a == "*" {
					star = append(star, r)
				} else if a != "" && strings.Contains(agent, a) {
					ours = append(ours, r)
				}
			}
		case "content-signal":
			for _, pair := range strings.Split(val, ",") {
				k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
				if !ok {
					continue
				}
				k, v = strings.ToLower(strings.TrimSpace(k)), strings.ToLower(strings.TrimSpace(v))
				for _, a := range cur {
					if a == "*" {
						starSig[k] = v
					} else if a != "" && strings.Contains(agent, a) {
						oursSig[k] = v
					}
				}
			}
		case "sitemap":
			if val != "" {
				sitemaps = append(sitemaps, val)
			}
		}
	}
	rb := &robots{sitemaps: sitemaps}
	if len(ours) > 0 || len(oursSig) > 0 {
		rb.rules, rb.noSearch = ours, oursSig["search"] == "no"
	} else {
		rb.rules, rb.noSearch = star, starSig["search"] == "no"
	}
	return rb
}

// allowed applies the longest matching rule; on a tie, allow wins.
func (r *robots) allowed(path string) bool {
	if r == nil {
		return true
	}
	if r.noSearch {
		return false
	}
	best, ok := -1, true
	for _, x := range r.rules {
		if x.pat == "" || strings.Count(x.pat, "*") > 5 {
			continue
		}
		if wildMatch(x.pat, path) {
			if l := len(x.pat); l > best || (l == best && x.allow) {
				best, ok = l, x.allow
			}
		}
	}
	return ok
}

func wildMatch(pat, s string) bool {
	end := strings.HasSuffix(pat, "$")
	if end {
		pat = pat[:len(pat)-1]
	}
	return match(pat, s, end)
}

func match(p, s string, end bool) bool {
	for len(p) > 0 {
		if p[0] == '*' {
			for len(p) > 0 && p[0] == '*' {
				p = p[1:]
			}
			if len(p) == 0 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if match(p, s[i:], end) {
					return true
				}
			}
			return false
		}
		if len(s) == 0 || s[0] != p[0] {
			return false
		}
		p, s = p[1:], s[1:]
	}
	return !end || len(s) == 0
}
