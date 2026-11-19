#!/usr/bin/env python3
"""Which Lemmy communities are NSFW? Communities only, no posts.

Reads a list of forum hosts (one per line). For each host that runs Lemmy it reads the host's public community list
(GET /api/v3/community/list, type Local, with show_nsfw=true) and keeps the communities Lemmy itself marks `nsfw`.
Every community is listed by the server it lives on (type Local), so each one is seen once.

Polite: one host is read by one thread, 0.5 s between pages, 50 communities a page, an honest user agent.

  lemmy_nsfw_communities.py hosts.txt outdir [--max-hosts N] [--max-pages N]

writes in outdir:
  nsfw-communities.txt   one community name per line (lowercase, without c/): what Flowsearch hides
  nsfw-instances.txt     hosts where almost every community is NSFW (a suggestion for the host block list; not applied by itself)
  report.json            how many hosts, Lemmy hosts, communities, NSFW communities, and which hosts failed
"""
import concurrent.futures, json, os, sys, time, urllib.error, urllib.request

UA = "MySearchBot/1.0 (+https://flowsearch.org/crawling)"
PAGE = 50
DELAY = 0.5


def get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        if r.status != 200:
            raise urllib.error.HTTPError(url, r.status, "status", r.headers, None)
        return json.loads(r.read(8_000_000))


def read_host(host, max_pages):
    """-> (is_lemmy, communities_seen, [nsfw community names], error text or '')"""
    try:
        site = get(f"https://{host}/api/v3/site", timeout=15)
    except Exception as e:
        return False, 0, [], f"not Lemmy or not reachable: {type(e).__name__}"
    if not isinstance(site, dict) or "site_view" not in site:
        return False, 0, [], "not Lemmy"
    seen, nsfw, err = 0, [], ""
    for page in range(1, max_pages + 1):
        try:
            d = get(f"https://{host}/api/v3/community/list?type_=Local&sort=New&limit={PAGE}&page={page}&show_nsfw=true")
        except Exception as e:
            err = f"page {page}: {type(e).__name__}"
            break
        items = d.get("communities") or []
        if not items:
            break
        for it in items:
            c = it.get("community") or {}
            seen += 1
            if c.get("nsfw") and c.get("name"):
                nsfw.append(c["name"].lower())
        if len(items) < PAGE:
            break
        time.sleep(DELAY)
    return True, seen, nsfw, err


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    opts = dict(a.split("=", 1) for a in sys.argv[1:] if a.startswith("--") and "=" in a)
    if len(args) != 2:
        sys.exit(__doc__)
    hosts = [h.strip().lower() for h in open(args[0]) if h.strip() and not h.startswith("#")]
    if "--max-hosts" in opts:
        hosts = hosts[: int(opts["--max-hosts"])]
    max_pages = int(opts.get("--max-pages", 400))
    os.makedirs(args[1], exist_ok=True)
    names, nsfw_hosts, report = set(), [], {"hosts": len(hosts), "lemmy_hosts": 0, "communities": 0, "nsfw_communities": 0, "failed": {}, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(read_host, h, max_pages): h for h in hosts}
        for f in concurrent.futures.as_completed(futs):
            h = futs[f]
            done += 1
            try:
                is_lemmy, seen, nsfw, err = f.result()
            except Exception as e:
                is_lemmy, seen, nsfw, err = False, 0, [], f"crashed: {e}"
            if is_lemmy:
                report["lemmy_hosts"] += 1
                report["communities"] += seen
                report["nsfw_communities"] += len(nsfw)
                names.update(nsfw)
                if seen >= 5 and len(nsfw) / seen >= 0.8:
                    nsfw_hosts.append(h)
                if err:
                    report["failed"][h] = err
            if done % 100 == 0:
                print(f"{done}/{len(hosts)} hosts, {report['lemmy_hosts']} Lemmy, {len(names)} NSFW names so far", flush=True)
    report["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(os.path.join(args[1], "nsfw-communities.txt"), "w") as f:
        f.write("# Lemmy communities that the Lemmy servers themselves mark NSFW (made by lemmy_nsfw_communities.py), one name per line\n")
        f.write("\n".join(sorted(names)) + "\n")
    with open(os.path.join(args[1], "nsfw-instances.txt"), "w") as f:
        f.write("# hosts where at least 80% of the (5 or more) communities are NSFW: a suggestion for the host block list\n")
        f.write("\n".join(sorted(nsfw_hosts)) + "\n")
    json.dump(report, open(os.path.join(args[1], "report.json"), "w"), indent=1)
    print(json.dumps({k: v for k, v in report.items() if k != "failed"}), flush=True)


if __name__ == "__main__":
    main()
