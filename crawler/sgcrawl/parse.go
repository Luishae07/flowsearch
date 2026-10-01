package main

import (
	"bytes"
	"html"
	"strings"
)

// parsed is what the crawler keeps of a web page.
type parsed struct {
	title, desc, text string
	links             []string
}

var skipTags = map[string]bool{"script": true, "style": true, "noscript": true, "svg": true, "template": true, "iframe": true, "object": true}
var chromeTags = map[string]bool{"nav": true, "header": true, "footer": true, "aside": true}
var spaceTags = map[string]bool{"p": true, "div": true, "br": true, "li": true, "tr": true, "td": true, "th": true,
	"h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true, "section": true, "article": true,
	"ul": true, "ol": true, "table": true, "dd": true, "dt": true, "blockquote": true, "pre": true, "form": true, "main": true}

func isNameChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == ':' || c == '_'
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' }

// indexFold finds the closing tag pat (lower case, like "</script") without caring about case.
func indexFold(b []byte, pat string) int {
	i := 0
	for {
		j := bytes.Index(b[i:], []byte("</"))
		if j < 0 {
			return -1
		}
		i += j
		if i+len(pat) <= len(b) && strings.EqualFold(string(b[i:i+len(pat)]), pat) {
			return i
		}
		i += 2
	}
}

// attrValue returns the value of one attribute from the inside of a tag.
func attrValue(a []byte, name string) string {
	i, n := 0, len(a)
	for i < n {
		for i < n && (isSpace(a[i]) || a[i] == '/') {
			i++
		}
		s := i
		for i < n && a[i] != '=' && !isSpace(a[i]) && a[i] != '/' && a[i] != '>' {
			i++
		}
		key := a[s:i]
		for i < n && isSpace(a[i]) {
			i++
		}
		if i < n && a[i] == '=' {
			i++
			for i < n && isSpace(a[i]) {
				i++
			}
			var val []byte
			if i < n && (a[i] == '"' || a[i] == '\'') {
				q := a[i]
				i++
				s2 := i
				for i < n && a[i] != q {
					i++
				}
				val = a[s2:i]
				if i < n {
					i++
				}
			} else {
				s2 := i
				for i < n && !isSpace(a[i]) && a[i] != '>' {
					i++
				}
				val = a[s2:i]
			}
			if strings.EqualFold(string(key), name) {
				return string(val)
			}
		} else if i == s {
			i++
		}
	}
	return ""
}

func collapse(s string) string { return strings.Join(strings.Fields(s), " ") }

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > n {
		return string(r[:n])
	}
	return s
}

// parse reads the page in one pass. It takes the title, the meta description, the visible text (skipping
// scripts, styles, menus, headers and footers) and, if asked, the links. maxText stops collecting text early.
func parse(b []byte, wantLinks bool, maxText int) parsed {
	var p parsed
	var text, title []byte
	chrome, inHead, inTitle := 0, false, false
	n, i := len(b), 0
	for i < n {
		lt := bytes.IndexByte(b[i:], '<')
		if lt < 0 {
			lt = n - i
		}
		if seg := b[i : i+lt]; len(seg) > 0 {
			if inTitle {
				title = append(title, seg...)
			} else if chrome == 0 && !inHead && len(text) < maxText {
				text = append(text, seg...)
			}
		}
		i += lt
		if i >= n {
			break
		}
		if !wantLinks && len(text) >= maxText && p.desc != "" {
			break // enough: nothing more is needed from the rest of the page
		}
		if bytes.HasPrefix(b[i:], []byte("<!--")) {
			e := bytes.Index(b[i+4:], []byte("-->"))
			if e < 0 {
				break
			}
			i += 4 + e + 3
			continue
		}
		j := i + 1
		closing := false
		if j < n && b[j] == '/' {
			closing = true
			j++
		}
		ns := j
		for j < n && isNameChar(b[j]) {
			j++
		}
		if j == ns { // a lone '<' in the text
			if chrome == 0 && !inHead && !inTitle && len(text) < maxText {
				text = append(text, '<')
			}
			i++
			continue
		}
		name := strings.ToLower(string(b[ns:j]))
		k := j
		var q byte
		for k < n {
			c := b[k]
			if q != 0 {
				if c == q {
					q = 0
				}
			} else if c == '"' || c == '\'' {
				q = c
			} else if c == '>' {
				break
			}
			k++
		}
		if k >= n {
			break
		}
		attrs := b[j:k]
		selfClose := k > j && b[k-1] == '/'
		i = k + 1
		switch {
		case skipTags[name]:
			if !closing && !selfClose {
				if e := indexFold(b[i:], "</"+name); e < 0 {
					i = n
				} else {
					i += e
				}
			}
		case name == "title":
			inTitle = !closing
		case name == "head":
			inHead = !closing
		case name == "body":
			inHead = false
		case chromeTags[name]:
			if closing {
				if chrome > 0 {
					chrome--
				}
			} else if !selfClose {
				chrome++
			}
		case name == "meta" && !closing:
			if p.desc == "" {
				nm := strings.ToLower(attrValue(attrs, "name"))
				pr := strings.ToLower(attrValue(attrs, "property"))
				if nm == "description" || nm == "twitter:description" || pr == "og:description" {
					p.desc = attrValue(attrs, "content")
				}
			}
		case name == "a" && !closing && wantLinks:
			if h := attrValue(attrs, "href"); h != "" {
				p.links = append(p.links, html.UnescapeString(h))
			}
		case spaceTags[name]:
			if chrome == 0 && !inHead && len(text) < maxText {
				text = append(text, ' ')
			}
		}
	}
	p.title = clip(collapse(html.UnescapeString(string(title))), 200)
	p.desc = clip(collapse(html.UnescapeString(p.desc)), 300)
	p.text = clip(collapse(html.UnescapeString(string(text))), 2000)
	return p
}
