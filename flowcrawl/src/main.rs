// flowcrawl: the Flowsearch crawler, written for GitHub Actions.
//
//   flowcrawl --seeds FILE --start N --count 500 [--pages 10] [--conc 500] [--ingest URL] [--render 4]
//
// One batch = `count` sites of the seed list. Every site is read at the same time as all the others (--conc),
// each site politely, one request after the other. For a site:
//   1. robots.txt first (a page it disallows for "MySearchBot" is never read),
//   2. up to --pages pages of that site only (links to other sites are ignored),
//   3. a page is read with a plain request first (fast). If it has almost no text (a page that builds itself with JavaScript),
//      it is opened in a real headless Firefox instead (--render Firefox windows, started through geckodriver).
// At the end ALL the pages of the batch go to the Flowsearch ingest in ONE request: POST URL/api/ingest, Bearer token from $MYSEARCH_TOKEN.

use fantoccini::{ClientBuilder, Locator};
use futures::stream::{self, StreamExt};
use scraper::{node::Node, Html, Selector};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use url::Url;

const AGENT: &str = "MySearchBot";
const UA: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0 (compatible; MySearchBot/1.0; +https://github.com/Luishae07/flowsearch)";
const SKIP_EXT: &[&str] = &[
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".zip", ".gz", ".mp4", ".mp3", ".css", ".js", ".xml", ".ico", ".woff", ".woff2", ".exe", ".dmg", ".apk",
];
const BAD_TITLES: &[&str] = &["just a moment", "attention required", "access denied", "are you a robot"];
const MAX_BODY: usize = 2 * 1024 * 1024;
const THIN: usize = 200; // fewer characters than this: the page probably needs JavaScript

const JS: &str = r#"var d=document, m=d.querySelector('meta[name=description]')||d.querySelector('meta[property="og:description"]');
var links=[].slice.call(d.querySelectorAll('a[href]')).map(function(a){return a.href}).slice(0,400);
return JSON.stringify({title:d.title||'',desc:m?m.content:'',text:(d.body?d.body.innerText:'').slice(0,6000),links:links});"#;

#[derive(Serialize, Clone)]
struct Page {
    url: String,
    title: String,
    description: String,
    text: String,
    source: String,
}

#[derive(Deserialize, Default)]
struct Seen {
    #[serde(default)]
    title: String,
    #[serde(default)]
    desc: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    links: Vec<String>,
}

struct Args {
    seeds: String,
    start: usize,
    count: usize,
    pages: usize,
    conc: usize,
    render: usize,
    ingest: String,
}

fn parse_args() -> Args {
    let mut a = Args { seeds: String::new(), start: 0, count: 500, pages: 10, conc: 500, render: 4, ingest: String::new() };
    let v: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i + 1 < v.len() {
        match v[i].as_str() {
            "--seeds" => a.seeds = v[i + 1].clone(),
            "--start" => a.start = v[i + 1].parse().unwrap_or(0),
            "--count" => a.count = v[i + 1].parse().unwrap_or(500),
            "--pages" => a.pages = v[i + 1].parse().unwrap_or(10),
            "--conc" => a.conc = v[i + 1].parse().unwrap_or(500),
            "--render" => a.render = v[i + 1].parse().unwrap_or(4),
            "--ingest" => a.ingest = v[i + 1].clone(),
            _ => {}
        }
        i += 2;
    }
    if a.seeds.is_empty() {
        eprintln!("usage: flowcrawl --seeds FILE --start N --count 500 [--pages 10] [--conc 500] [--render 4] [--ingest URL]");
        std::process::exit(2);
    }
    a
}

fn trunc(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

fn bare_host(u: &Url) -> String {
    u.host_str().unwrap_or("").to_lowercase().trim_start_matches("www.").to_string()
}

/// the text a person would read: everything except script, style, head and template content
fn page_text(doc: &Html) -> String {
    let mut out = String::new();
    for node in doc.tree.nodes() {
        if let Node::Text(t) = node.value() {
            let mut skip = false;
            let mut p = node.parent();
            while let Some(pn) = p {
                if let Node::Element(e) = pn.value() {
                    if matches!(e.name(), "script" | "style" | "noscript" | "template" | "head" | "title" | "svg") {
                        skip = true;
                        break;
                    }
                }
                p = pn.parent();
            }
            if !skip {
                out.push_str(t);
                out.push(' ');
                if out.len() > 12000 {
                    break;
                }
            }
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

async fn fetch_html(client: &reqwest::Client, url: &str) -> Option<String> {
    let mut resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let ct = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("").to_lowercase();
    if !ct.is_empty() && !ct.contains("html") {
        return None;
    }
    let mut body: Vec<u8> = Vec::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_BODY {
            break;
        }
    }
    Some(String::from_utf8_lossy(&body).into_owned())
}

type Job = (String, oneshot::Sender<Option<Seen>>);

/// the Firefox windows: each one is a geckodriver + a headless Firefox, taking page addresses from one shared queue
async fn start_firefox(n: usize) -> Option<mpsc::Sender<Job>> {
    if n == 0 {
        return None;
    }
    let (tx, rx) = mpsc::channel::<Job>(256);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    let driver = std::env::var("GECKOWEBDRIVER").map(|d| format!("{}/geckodriver", d)).unwrap_or_else(|_| "geckodriver".to_string());
    let mut up = 0;
    for i in 0..n {
        let port = 4444 + i as u16;
        let child = tokio::process::Command::new(&driver).arg("--port").arg(port.to_string()).kill_on_drop(true).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn();
        let Ok(child) = child else {
            eprintln!("geckodriver did not start");
            continue;
        };
        std::mem::forget(child);
        tokio::time::sleep(Duration::from_millis(800)).await;
        let mut caps = serde_json::Map::new();
        caps.insert("moz:firefoxOptions".to_string(), serde_json::json!({"args": ["-headless"], "prefs": {"permissions.default.image": 2}}));
        let client = match ClientBuilder::native().capabilities(caps).connect(&format!("http://localhost:{}", port)).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Firefox window {} did not open: {}", i, e);
                continue;
            }
        };
        up += 1;
        let rx = rx.clone();
        tokio::spawn(async move {
            loop {
                let job = { rx.lock().await.recv().await };
                let Some((url, back)) = job else { break };
                let r = tokio::time::timeout(Duration::from_secs(40), async {
                    client.goto(&url).await.ok()?;
                    tokio::time::sleep(Duration::from_millis(1800)).await;
                    let v = client.execute(JS, vec![]).await.ok()?;
                    serde_json::from_str::<Seen>(v.as_str()?).ok()
                })
                .await
                .ok()
                .flatten();
                let _ = back.send(r);
            }
            let _ = client.close().await;
        });
    }
    let _ = Locator::Css("body"); // keeps the import used on every fantoccini version
    println!("{} Firefox windows are open", up);
    if up == 0 {
        None
    } else {
        Some(tx)
    }
}

async fn render(ff: &Option<mpsc::Sender<Job>>, url: &str) -> Option<Seen> {
    let tx = ff.as_ref()?;
    let (b, r) = oneshot::channel();
    tx.send((url.to_string(), b)).await.ok()?;
    r.await.ok().flatten()
}

async fn crawl_site(client: reqwest::Client, ff: Option<mpsc::Sender<Job>>, seed: String, pages: usize, out: Arc<Mutex<Vec<Page>>>) -> usize {
    let seed = if seed.starts_with("http://") || seed.starts_with("https://") { seed } else { format!("https://{}", seed) };
    let Ok(start) = Url::parse(&seed) else { return 0 };
    let host = bare_host(&start);
    if host.is_empty() {
        return 0;
    }
    let base = format!("{}://{}", start.scheme(), start.host_str().unwrap_or(""));

    let mut delay = 0.7f32;
    let mut robot: Option<texting_robots::Robot> = None;
    if let Ok(r) = client.get(format!("{}/robots.txt", base)).timeout(Duration::from_secs(10)).send().await {
        if r.status().is_success() {
            if let Ok(b) = r.bytes().await {
                if let Ok(rb) = texting_robots::Robot::new(AGENT, &b) {
                    if let Some(d) = rb.delay {
                        delay = d.clamp(0.5, 5.0);
                    }
                    robot = Some(rb);
                }
            }
        }
    }
    let allowed = |u: &str| robot.as_ref().map(|r| r.allowed(u)).unwrap_or(true);
    if !allowed(&seed) {
        println!("{host}: robots.txt asks crawlers to stay away: skipped");
        return 0;
    }

    let sel_title = Selector::parse("title").unwrap();
    let sel_desc = Selector::parse("meta[name=description], meta[property='og:description']").unwrap();
    let sel_a = Selector::parse("a[href]").unwrap();

    let mut queue: VecDeque<String> = VecDeque::new();
    let mut seen: HashSet<String> = HashSet::new();
    queue.push_back(seed.clone());
    seen.insert(seed.clone());
    let mut sent = 0usize;
    let mut tries = 0usize;
    let mut js_pages = 0usize;

    while let Some(url) = queue.pop_front() {
        if sent >= pages || tries >= pages * 3 {
            break;
        }
        tries += 1;
        let mut got: Option<Seen> = None;
        if let Some(html) = fetch_html(&client, &url).await {
            let doc = Html::parse_document(&html);
            let title = doc.select(&sel_title).next().map(|t| t.text().collect::<String>()).unwrap_or_default();
            let desc = doc.select(&sel_desc).next().and_then(|m| m.value().attr("content")).unwrap_or("").to_string();
            let text = page_text(&doc);
            let links: Vec<String> = doc.select(&sel_a).filter_map(|a| a.value().attr("href")).map(|s| s.to_string()).take(400).collect();
            got = Some(Seen { title, desc, text, links });
        }
        // an empty or almost empty page (or none at all): open it in Firefox
        if got.as_ref().map(|g| g.text.chars().count() < THIN).unwrap_or(true) {
            if let Some(r) = render(&ff, &url).await {
                if r.text.chars().count() >= got.as_ref().map(|g| g.text.chars().count()).unwrap_or(0) {
                    js_pages += 1;
                    got = Some(r);
                }
            }
        }
        let Some(g) = got else {
            tokio::time::sleep(Duration::from_millis(300)).await;
            continue;
        };
        let title = g.title.split_whitespace().collect::<Vec<_>>().join(" ");
        let text = g.text.split_whitespace().collect::<Vec<_>>().join(" ");
        let low = title.to_lowercase();
        if !text.is_empty() && !BAD_TITLES.iter().any(|b| low.starts_with(b)) {
            let page = Page { url: url.clone(), title: trunc(&title, 250), description: trunc(g.desc.trim(), 400), text: trunc(&text, 3000), source: String::new() };
            if let Ok(mut w) = out.lock() {
                w.push(page);
            }
            sent += 1;
        }
        if let Ok(cur) = Url::parse(&url) {
            for href in &g.links {
                if queue.len() > 300 {
                    break;
                }
                let Ok(mut l) = cur.join(href) else { continue };
                l.set_fragment(None);
                if !(l.scheme() == "http" || l.scheme() == "https") || bare_host(&l) != host {
                    continue;
                }
                let ls = l.to_string();
                let path = l.path().to_lowercase();
                if SKIP_EXT.iter().any(|e| path.ends_with(e)) || seen.contains(&ls) || !allowed(&ls) {
                    continue;
                }
                seen.insert(ls.clone());
                queue.push_back(ls);
            }
        }
        tokio::time::sleep(Duration::from_millis((delay * 1000.0) as u64)).await;
    }
    println!("{host}: {sent} pages ({js_pages} through Firefox)");
    sent
}

#[tokio::main]
async fn main() {
    let a = parse_args();
    let text = std::fs::read_to_string(&a.seeds).expect("cannot read the seed file");
    let sites: Vec<String> = text
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .skip(a.start)
        .take(a.count)
        .map(|l| l.to_string())
        .collect();
    println!("{} sites (from number {}), up to {} pages each, {} at the same time", sites.len(), a.start, a.pages, a.conc);

    let client = reqwest::Client::builder()
        .user_agent(UA)
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::limited(5))
        .pool_max_idle_per_host(1)
        .build()
        .expect("http client");
    let ff = start_firefox(a.render).await;
    let out: Arc<Mutex<Vec<Page>>> = Arc::new(Mutex::new(Vec::new()));

    let pages = a.pages;
    let t0 = std::time::Instant::now();
    let total: usize = stream::iter(sites)
        .map(|s| {
            let (c, o, f) = (client.clone(), out.clone(), ff.clone());
            async move { tokio::time::timeout(Duration::from_secs(300), crawl_site(c, f, s, pages, o)).await.unwrap_or(0) }
        })
        .buffer_unordered(a.conc)
        .fold(0usize, |acc, n| async move { acc + n })
        .await;
    println!("crawled: {} pages in {} s", total, t0.elapsed().as_secs());

    let all: Vec<Page> = out.lock().map(|g| g.clone()).unwrap_or_default();
    std::fs::write("pages.json", serde_json::to_vec(&all).unwrap_or_default()).ok();
    if a.ingest.is_empty() {
        println!("no --ingest given: the pages are in pages.json");
        return;
    }
    if all.is_empty() {
        println!("nothing to send");
        return;
    }
    // ONE request with every page of the batch
    let token = std::env::var("MYSEARCH_TOKEN").unwrap_or_default();
    let url = format!("{}/api/ingest", a.ingest.trim_end_matches('/'));
    let body = serde_json::to_vec(&all).unwrap_or_default();
    println!("sending {} pages ({} KB) in one request to {}", all.len(), body.len() / 1024, url);
    let mut ok = false;
    for attempt in 1..=3 {
        match client.post(&url).bearer_auth(&token).header("Content-Type", "application/json").body(body.clone()).timeout(Duration::from_secs(300)).send().await {
            Ok(r) if r.status().is_success() => {
                println!("ingest accepted the batch (HTTP {})", r.status());
                ok = true;
                break;
            }
            Ok(r) => eprintln!("attempt {}: the ingest answered HTTP {}", attempt, r.status()),
            Err(e) => eprintln!("attempt {}: the ingest could not be reached: {}", attempt, e),
        }
        tokio::time::sleep(Duration::from_secs(5 * attempt)).await;
    }
    if !ok {
        std::process::exit(1);
    }
}
