#!/usr/bin/env python3
"""What does a visitor cost each site's owner? For every site in crawler/sites (plus Flowsearch's biggest
indexed sites, when its public tunnel answers) this loads the home page the way a browser would
(HTML + the images, scripts, styles and fonts it links to, up to 25), adds up the bytes, and turns that
into money with a simple cloud-bandwidth price. Writes site-costs.json and SITE-COSTS.md.

  EGRESS_PER_GB     price of one GB sent out        (default 0.085 USD, a typical cloud/CDN list price)
  VISITS_PER_HOUR   assumed visitors per hour       (default 1000)
This is bandwidth only: servers, storage and people cost extra, so real cost is higher.
"""
import concurrent.futures as cf
import glob
import gzip
import json
import os
import re
import urllib.parse
import urllib.request

EGRESS = float(os.environ.get("EGRESS_PER_GB", "0.085"))
VISITS = int(os.environ.get("VISITS_PER_HOUR", "1000"))
UA = {"User-Agent": "flowsearch-sitecost/1.0 (+https://github.com/Luishae07/flowsearch)", "Accept-Encoding": "gzip"}
ASSET = re.compile(r'(?:src|href)=["\']([^"\']+\.(?:js|css|png|jpe?g|gif|webp|avif|svg|ico|woff2?)(?:\?[^"\']*)?)["\']', re.I)


def fetch(url, head=False, limit=3_000_000):
    req = urllib.request.Request(url, headers=UA, method="HEAD" if head else "GET")
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.headers, (b"" if head else r.read(limit)), r.geturl()


def measure(host):
    try:
        h, body, final = fetch("https://" + host + "/")
    except Exception as e:
        return {"host": host, "ok": False, "error": str(e)[:60]}
    transferred = int(h.get("Content-Length") or len(body))
    try:
        text = (gzip.decompress(body) if h.get("Content-Encoding") == "gzip" else body).decode("utf8", "ignore")
    except Exception:
        text = body.decode("utf8", "ignore")
    refs = sorted({urllib.parse.urljoin(final, r) for r in ASSET.findall(text)})[:25]
    total, counted = transferred, 0

    def size(u):
        try:
            hh, _, _ = fetch(u, head=True)
            return int(hh.get("Content-Length") or 0)
        except Exception:
            return 0

    with cf.ThreadPoolExecutor(6) as ex:
        for s in ex.map(size, refs):
            total += s
            counted += 1 if s else 0
    per_visit = total / 1e9 * EGRESS
    return {"host": host, "ok": True, "html_bytes": transferred, "assets": counted, "bytes_per_visit": total,
            "usd_per_1000_visits": round(per_visit * 1000, 4), "usd_per_hour": round(per_visit * VISITS, 4)}


def main():
    hosts = {os.path.basename(p)[:-4]: None for p in sorted(glob.glob("crawler/sites/*.txt"))}
    try:  # biggest indexed sites, from the live engine (best effort)
        base = urllib.request.urlopen("https://raw.githubusercontent.com/Luishae07/flowsearch/main/web-tunnel-url.txt", timeout=10).read().decode().strip()
        stats = json.load(urllib.request.urlopen(urllib.request.Request(base + "/api/stats", headers=UA), timeout=15))
        for s in stats.get("top_sites", []):
            hosts[s["host"]] = s["pages"]
    except Exception as e:
        print("live stats not available:", e)
    with cf.ThreadPoolExecutor(8) as ex:
        rows = list(ex.map(measure, hosts))
    for r in rows:
        r["pages_indexed"] = hosts[r["host"]]
    ok = sorted([r for r in rows if r["ok"]], key=lambda r: -r["usd_per_hour"])
    bad = [r for r in rows if not r["ok"]]
    json.dump({"egress_usd_per_gb": EGRESS, "visits_per_hour": VISITS, "sites": ok, "unreachable": bad},
              open("site-costs.json", "w"), indent=1)
    md = [f"# What a visitor costs each site\n",
          f"Bandwidth only, at **${EGRESS}/GB** and **{VISITS:,} visits per hour**. Every site's home page is loaded like a "
          "browser would (HTML plus up to 25 images, scripts, styles and fonts). Servers, storage and people cost extra.\n",
          "| Site | Page weight | Files | $ per 1,000 visits | $ per hour |", "|---|---:|---:|---:|---:|"]
    for r in ok:
        md.append(f"| {r['host']} | {r['bytes_per_visit']/1024:,.0f} KB | {r['assets']+1} | {r['usd_per_1000_visits']:.4f} | {r['usd_per_hour']:.4f} |")
    if bad:
        md += ["", f"Could not load: {', '.join(r['host'] for r in bad)}"]
    open("SITE-COSTS.md", "w").write("\n".join(md) + "\n")
    print(f"{len(ok)} measured, {len(bad)} unreachable")


if __name__ == "__main__":
    main()
