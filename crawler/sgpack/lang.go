package sgpack

import (
	"net/url"
	"strings"
	"unicode"
)

var englishWords = map[string]bool{}

func init() {
	for _, w := range strings.Fields("the of and to in is that for it with as on are this be by at from or an was have has not but you your we can will they their its which who what when where how all more one about into than then also our these those been were") {
		englishWords[w] = true
	}
}

var foreignTLDs = []string{".de", ".fr", ".es", ".it", ".nl", ".pl", ".ru", ".jp", ".cn", ".br", ".pt", ".se", ".no", ".dk", ".fi", ".cz", ".tr", ".kr", ".at", ".ch", ".gr", ".ua", ".hu", ".ro"}

// IsEnglish says whether a page is (probably) English: not mostly another script, and with the usual share of
// common English words. Short pages are given the benefit of the doubt; sites from a country with another
// language have to look more clearly English.
func IsEnglish(pageURL, title, description, text string) bool {
	if len(text) > 600 {
		text = text[:600]
	}
	all := title + " " + description + " " + text
	letters, latin := 0, 0
	for _, r := range all {
		if unicode.IsLetter(r) {
			letters++
			if r <= 0x24F {
				latin++
			}
		}
	}
	if letters == 0 {
		return true
	}
	if float64(latin)/float64(letters) < 0.8 { // Chinese, Cyrillic, Arabic ...
		return false
	}
	words := strings.FieldsFunc(strings.ToLower(all), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) })
	if len(words) < 15 {
		return true
	}
	hits := 0
	for _, w := range words {
		if englishWords[w] {
			hits++
		}
	}
	need := 0.07
	if u, err := url.Parse(pageURL); err == nil {
		for _, tld := range foreignTLDs {
			if strings.HasSuffix(u.Hostname(), tld) {
				need = 0.14
				break
			}
		}
	}
	return float64(hits)/float64(len(words)) >= need
}

var germanWords = map[string]bool{}

func init() {
	for _, w := range strings.Fields("der die das und ist nicht ein eine mit von zu den für auf im dem des sich auch es an als wir sie ich aber bei oder wird nach wie über aus noch nur wenn sind dass kann zur zum um durch uns ihr mehr unsere haben werden") {
		germanWords[w] = true
	}
}

// GermanURL: the address says "German": a German-speaking country's domain (.de .at .ch .li), a de.example.com
// subdomain, or a first folder like /de/, /de-de/, /de_AT/.
func GermanURL(pageURL string) bool {
	u, err := url.Parse(pageURL)
	if err != nil {
		return false
	}
	h := strings.ToLower(u.Hostname())
	for _, t := range []string{".de", ".at", ".ch", ".li"} {
		if strings.HasSuffix(h, t) {
			return true
		}
	}
	if strings.HasPrefix(h, "de.") || strings.HasPrefix(h, "www.de.") {
		return true
	}
	seg := strings.ToLower(strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)[0])
	switch seg {
	case "de", "de-de", "de_de", "de-at", "de_at", "de-ch", "de_ch", "deu", "german", "deutsch":
		return true
	}
	return false
}

// IsGerman says whether a page is addressed as German (GermanURL) and is not plainly in another language.
// Short pages are given the benefit of the doubt.
func IsGerman(pageURL, title, description, text string) bool {
	if !GermanURL(pageURL) {
		return false
	}
	if len(text) > 600 {
		text = text[:600]
	}
	words := strings.FieldsFunc(strings.ToLower(title+" "+description+" "+text), func(r rune) bool { return !unicode.IsLetter(r) })
	if len(words) < 15 {
		return true
	}
	hits := 0
	for _, w := range words {
		if germanWords[w] {
			hits++
		}
	}
	return float64(hits)/float64(len(words)) >= 0.08
}

// IsGermanText: the text itself is German (for a site whose owner said it is German, whatever its address).
func IsGermanText(pageURL, title, description, text string) bool {
	if len(text) > 600 {
		text = text[:600]
	}
	words := strings.FieldsFunc(strings.ToLower(title+" "+description+" "+text), func(r rune) bool { return !unicode.IsLetter(r) })
	if len(words) < 15 {
		return false
	}
	hits := 0
	for _, w := range words {
		if germanWords[w] {
			hits++
		}
	}
	return float64(hits)/float64(len(words)) >= 0.10
}
