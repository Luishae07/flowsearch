const $ = id => document.getElementById(id);
const esc = s => String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

(async () => {
  const [tab] = await API.tabs.query({ active: true, currentWindow: true });
  const host = tab && siteOf(tab.url);
  if (!host) {
    $("site").innerHTML = '<span class="dim">Open a website and click the icon again.</span>';
    return;
  }
  $("host").textContent = host;
  let base;
  try { base = await flowBase(); }
  catch (e) { $("site").innerHTML = '<span class="bad">Flowsearch is not reachable right now.</span>'; return; }
  $("searchSite").href = `${base}/search?q=${encodeURIComponent("site:" + host)}`;
  $("add").href = `${base}/webmaster?view=add`;

  try {
    const info = await siteInfo(host);
    if (!info.total) {
      $("site").innerHTML = `<div id="big" class="bad">Not indexed</div><p class="dim" style="margin:6px 0 0">Flowsearch has no pages of ${esc(host)} yet. “Add / recrawl” opens the webmaster tools, where you can add it.</p>`;
    } else {
      $("site").innerHTML = `<div id="big" class="ok">${countLabel(info.total)} pages</div><p class="dim" style="margin:4px 0 8px">of ${esc(host)} are in Flowsearch${info.total >= 100 ? " (it counts up to 100)" : ""}.</p><div id="pages">${info.pages.map(p => `<a href="${esc(p.url)}" title="${esc(p.url)}" target="_blank" rel="noopener">${esc(p.title || p.url)}</a>`).join("")}</div>`;
    }
    if (info.total) {
      let there = false;
      try { there = await pageIndexed(tab.url, tab.title, host); } catch (e) { /* leave it as unknown */ }
      $("page").hidden = false;
      $("page").innerHTML = there
        ? '<b class="ok">This page is indexed.</b>'
        : '<b>This exact page was not among the top results.</b><p class="dim" style="margin:4px 0 0">It may still be indexed: Flowsearch only shows the best matches for the page’s title.</p>';
    }
  } catch (e) {
    $("site").innerHTML = '<span class="bad">Could not ask Flowsearch right now.</span>';
  }
})();
