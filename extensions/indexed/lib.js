// Where Flowsearch lives right now (its tunnel address changes, so it is looked up in the pointer file
// the server publishes on GitHub and cached for 10 minutes), and the "is this site indexed" lookups.
const API = globalThis.browser || globalThis.chrome;
const FLOW_POINTER = "https://raw.githubusercontent.com/Luishae07/flowsearch/main/web-tunnel-url.txt";

async function cached(key, ttlMs, load) {
  const got = (await API.storage.local.get(key))[key];
  if (got && Date.now() - got.t < ttlMs) return got.v;
  try {
    const v = await load();
    await API.storage.local.set({ [key]: { t: Date.now(), v } });
    return v;
  } catch (e) {
    if (got) return got.v;
    throw e;
  }
}

const flowBase = () => cached("flowBase", 10 * 60e3, async () => {
  const r = await fetch(FLOW_POINTER, { cache: "no-store" });
  if (!r.ok) throw new Error("Flowsearch address unavailable");
  return (await r.text()).trim().replace(/\/$/, "");
});

async function search(q, perPage) {
  const base = await flowBase();
  const r = await fetch(`${base}/api/search?q=${encodeURIComponent(q)}&per_page=${perPage}`);
  if (!r.ok) throw new Error("Flowsearch answered " + r.status);
  return r.json();
}

// the part of the address Flowsearch's site: search understands (a leading www. is dropped)
function siteOf(url) {
  try {
    const u = new URL(url);
    if (u.protocol !== "http:" && u.protocol !== "https:") return null;
    return u.hostname.replace(/^www\./, "");
  } catch (e) { return null; }
}

// how many pages of the site Flowsearch has (its site: search counts at most 100) and the best few
const siteInfo = host => cached("site:" + host, 10 * 60e3, async () => {
  const d = await search("site:" + host, 5);
  return { total: d.total || 0, pages: (d.results || []).map(x => ({ url: x.url, title: x.title })) };
});

const norm = u => u.replace(/#.*$/, "").replace(/^https?:\/\/(www\.)?/, "").replace(/\/$/, "");

// is this exact page indexed? Search the site for the page's title; if the page is among the answers, yes.
async function pageIndexed(url, title, host) {
  const d = await search(`site:${host} ${title || ""}`.trim(), 20);
  const me = norm(url);
  return (d.results || []).some(x => norm(x.url) === me);
}

function countLabel(n) { return n >= 100 ? "100+" : String(n); }
