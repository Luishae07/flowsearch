if (typeof importScripts === "function") importScripts("lib.js"); // Chrome service worker; Firefox lists lib.js itself

// the toolbar badge on each tab: how many pages of that site Flowsearch has (red 0 = not indexed)
async function badge(tabId, url) {
  const host = url && siteOf(url);
  if (!host) { API.action.setBadgeText({ tabId, text: "" }); return; }
  try {
    const info = await siteInfo(host);
    const text = info.total >= 100 ? "99+" : String(info.total);
    API.action.setBadgeText({ tabId, text });
    API.action.setBadgeBackgroundColor({ tabId, color: info.total ? "#15803d" : "#b91c1c" });
  } catch (e) { API.action.setBadgeText({ tabId, text: "" }); }
}
API.tabs.onUpdated.addListener((tabId, change, tab) => { if (change.status === "complete") badge(tabId, tab.url); });
API.tabs.onActivated.addListener(async ({ tabId }) => { badge(tabId, (await API.tabs.get(tabId)).url); });
