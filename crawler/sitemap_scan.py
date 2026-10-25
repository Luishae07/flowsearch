#!/usr/bin/env python3
"""Reads a site's robots.txt, finds the sitemaps it lists and counts what is inside them.
Writes sitemap-scans/<site>.json (read by the Flowsearch webmaster tools)."""
import gzip, html, json, os, re, sys, time, urllib.request, urllib.error

site = os.environ.get("SITE", "").strip().lower()
if not re.fullmatch(r"[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*\.[a-z]{2,}", site):
    sys.exit("bad site name")
UA = "Mozilla/5.0 (compatible; MySearchBot/1.0; +https://github.com/Luishae07/flowsearch)"
LOC = re.compile(r"<loc>\s*(.*?)\s*</loc>", re.S | re.I)
started = time.time()


def get(url, limit):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read(limit)


def read_sitemap(addr, keep):
    out = {"addr": addr}
    try:
        if not re.match(r"https?://", addr):
            raise ValueError("not an http(s) address")
        b = get(addr, 12 << 20)
        if b[:2] == b"\x1f\x8b":
            b = gzip.decompress(b)[:60 << 20]
        text = b.decode("utf-8", "replace")
        if "<urlset" not in text and "<sitemapindex" not in text and "<loc" not in text.lower():
            raise ValueError("this does not look like a sitemap")
        locs = [html.unescape(m) for m in LOC.findall(text)]
        out["index"] = "<sitemapindex" in text
        out["total"] = len(locs)
        out["locs"] = locs[:keep]
    except urllib.error.HTTPError as e:
        out["error"] = "the site answered with status %d" % e.code
    except Exception as e:
        out["error"] = str(e)[:160] or "could not be reached"
    return out


robots, robots_status, robots_code = "", "not found", 0
for scheme in ("https://", "http://"):
    try:
        robots = get(scheme + site + "/robots.txt", 512 << 10).decode("utf-8", "replace")
        robots_status, robots_code = "found", 200
        break
    except urllib.error.HTTPError as e:
        robots_code = e.code
        break
    except Exception:
        continue

listed, seen = [], set()
for line in robots.splitlines():
    m = re.match(r"\s*sitemap\s*:\s*(\S+)", line, re.I)
    if m and m.group(1) not in seen:
        seen.add(m.group(1))
        listed.append(m.group(1))
how = "robots" if listed else "none"
if not listed:  # robots.txt names none: try the usual file names
    for name in ("sitemap.xml", "sitemap_index.xml", "sitemap-index.xml", "wp-sitemap.xml", "sitemap.xml.gz", "sitemap.txt"):
        for scheme in ("https://",):
            r = read_sitemap(scheme + site + "/" + name, 0)
            if "error" not in r:
                listed.append(r["addr"])
                how = "guess"
                break

sitemaps = []
for addr in listed[:10]:
    r = read_sitemap(addr, 60)
    if r.get("index") and "error" not in r:
        kids = []
        for kid in read_sitemap(addr, 40)["locs"][:20]:
            if time.time() - started > 240:
                break
            k = read_sitemap(kid, 0)
            kids.append({"addr": kid, "total": k.get("total"), "error": k.get("error")})
        r["children"] = kids
    sitemaps.append(r)

os.makedirs("sitemap-scans", exist_ok=True)
with open("sitemap-scans/%s.json" % site, "w") as f:
    json.dump({"site": site, "scanned": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "robots": robots_status, "robots_code": robots_code, "robots_body": robots[:400000],
               "found_by": how, "listed": len(listed), "sitemaps": sitemaps}, f)
print("scanned", site, len(sitemaps), "sitemaps")
