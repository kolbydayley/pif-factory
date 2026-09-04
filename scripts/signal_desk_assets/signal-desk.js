(() => {
  "use strict";

  const app = document.querySelector("#app");
  const FILES = {
    index: "./signal-desk-index.json",
    issues: "./signal-desk-issues.json",
    voices: "./signal-desk-voices.json",
    network: "./signal-desk-network.json",
    coverage: "./pif-signal-desk-funnel.json",
  };
  const cache = new Map();
  const state = {
    briefingBucket: "all",
    issueQuery: "",
    issueSort: "strategic",
    issueConfidence: "all",
    voiceQuery: "",
    voiceView: "evidence",
    evidenceMode: "accepted",
    selectedMonth: "",
    coverageGap: "needs_attention",
    coverageQuery: "",
  };
  let INDEX = null;

  const esc = value => String(value ?? "").replace(/[&<>"]/g, char => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;",
  }[char]));
  const cap = value => String(value ?? "").replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())
    .replace(/\b(Ai|Agi|Us|Llm|Api|Ipo|Mcp|Rss|Css|Html|Htmx|Ipv6)\b/g, token => token.toUpperCase());
  const enc = value => encodeURIComponent(String(value));
  const safeUrl = value => /^https?:\/\//i.test(value || "") ? value : null;
  const fmt = value => Number(value || 0).toLocaleString();
  const pct = value => `${(Number(value || 0) * 100).toFixed(Number(value || 0) < .01 ? 2 : 1)}%`;
  const byId = id => document.getElementById(id);

  async function load(name) {
    if (cache.has(name)) return cache.get(name);
    const promise = fetch(FILES[name], {cache: "no-store", headers: {accept: "application/json"}})
      .then(response => {
        if (!response.ok) throw new Error(`${name} returned ${response.status}`);
        return response.json();
      });
    cache.set(name, promise);
    try { return await promise; }
    catch (error) { cache.delete(name); throw error; }
  }

  function route(kind, id = "", tail = []) {
    const parts = [kind, id, ...tail].filter(Boolean).map(enc);
    history.replaceState({...history.state, filters: {...state}, scroll: app.scrollTop || window.scrollY}, "");
    const origin=location.hash;
    location.hash=parts.join("/");
    history.replaceState({...history.state,origin,filters:{...state},scroll:0},"");
  }

  function parseRoute() {
    const parts = (location.hash.slice(1) || "briefing").split("/").map(part => {
      try { return decodeURIComponent(part); } catch (_) { return part; }
    });
    let [kind, id = "", filterType = "", filterValue = ""] = parts;
    const legacyHome = {shifts: "briefing", topics: "issues", people: "voices"};
    if (kind === "home") kind = legacyHome[id] || "briefing", id = "";
    if (kind === "topic") kind = "issue";
    if (kind === "person") kind = "voice";
    if (kind === "funnel") kind = "coverage";
    return {kind, id, filterType, filterValue};
  }

  function navState(kind) {
    document.querySelectorAll("[data-nav]").forEach(node => {
      const active = node.dataset.nav === kind;
      node.classList.toggle("active", active);
      if (active) node.setAttribute("aria-current", "page");
      else node.removeAttribute("aria-current");
    });
  }

  function breadcrumb(items) {
    return `<nav class="breadcrumbs" aria-label="Breadcrumb">${items.map((item, i) =>
      i === items.length - 1
        ? `<span aria-current="page">${esc(item.label)}</span>`
        : `<button data-route="${esc(item.route)}" data-id="${esc(item.id || "")}">${esc(item.label)}</button><span aria-hidden="true">/</span>`
    ).join("")}</nav>`;
  }

  function coverageFooter() {
    return `<footer class="coverage-footer">Research snapshot · Discourse through ${esc(INDEX?.data_through || "unknown")}. Bounded excerpts and source links; this legacy corpus is still under quality review. <button class="text-button" data-route="coverage">Inspect coverage and limitations</button></footer>`;
  }

  function snapshotNote() {
    return `<details class="snapshot-note"><summary>Research snapshot · ${esc(INDEX?.data_through || "date unavailable")} <span>Evidence under review</span></summary><p>The site currently uses legacy evidence. The clean-corpus rebuild is not yet approved for publication. Counts describe this sample, not the whole industry; attribution and surrounding context can be incomplete. Snapshot generated ${esc(INDEX?.generated_at || "unknown")}.</p></details>`;
  }

  function recentWindow(item) {
    const months=(INDEX?.months || []).slice(-3);
    return months.length ? `${months[0]}–${months.at(-1)}` : "latest recorded window";
  }

  function researchBucket(item) {
    if (!item.brief?.decision_grade) return "watchlist";
    if (!(Number(item.pulse_vol) > 0)) return "archive";
    return item.bucket === "changing_consensus" ? "distribution" : "active";
  }

  function excerptIdentity(evidence) {
    const name = evidence.person;
    return name && name !== "Unattributed voice" ? name : "Speaker not established";
  }

  function previewSeries(item) {
    return `<span class="chart-preview">${spark(item.series_preview,"var(--green)")}<small>Attention share · ${esc(item.series_preview?.[0]?.month || "")}–${esc(item.series_preview?.at(-1)?.month || "")}</small></span>`;
  }

  function readingLinks(items) {
    return `<nav class="reading-links" aria-label="On this page">${items.map(([id,label]) => `<button class="chip" data-jump="${esc(id)}">${esc(label)}</button>`).join("")}</nav>`;
  }

  function spark(series, color = "var(--blue)") {
    const points = (series || []).slice(-12);
    if (!points.length) return `<span class="spark" aria-hidden="true"></span>`;
    const values = points.map(point => point.share_smooth ?? point.share ?? 0);
    const max = Math.max(...values, .0001);
    return `<span class="spark" style="color:${color}" aria-hidden="true">${points.map((point, i) =>
      `<i style="height:${Math.max(3, Math.round(values[i] / max * 42))}px;opacity:${point.low_sample ? .28 : .82}"></i>`
    ).join("")}</span>`;
  }

  function coveragePill(brief, issueId) {
    const c = brief.coverage || {};
    return `<button class="coverage-badge" data-route="coverage" data-tail="issue/${esc(issueId)}">${fmt(c.accepted_excerpts)} displayed excerpts · ${fmt(c.shows)} shows</button>`;
  }

  function citationLinks(sentence) {
    const citations = sentence?.citations || [];
    return citations.map((id, i) =>
      `<button class="citation" data-route="evidence" data-id="${esc(id)}" title="Open supporting evidence" aria-label="Open citation ${i + 1}">${i + 1}</button>`
    ).join("");
  }

  function sentence(sentenceValue, className = "brief-line") {
    if (!sentenceValue?.text) return "";
    return `<p class="${className}">${esc(sentenceValue.text)}${citationLinks(sentenceValue)}</p>`;
  }

  const BUCKETS = [
    ["all", "Research overview"], ["active", "Recent discussion"],
    ["distribution", "Stance distributions"], ["archive", "Historical evidence"],
    ["watchlist", "Watchlist"],
  ];

  function signalCard(item) {
    const c = item.brief?.coverage || {};
    const bucket = researchBucket(item);
    return `<button class="signal-card ${bucket === "watchlist" ? "watchlist" : ""}" data-route="issue" data-id="${esc(item.id)}">
      <span class="signal-kind">${esc(BUCKETS.find(([id]) => id === bucket)?.[1] || "Research issue")}</span>
      <h3>${esc(cap(item.name))}</h3>
      <p>${fmt(item.pulse_vol)} mentions · ${esc(recentWindow(item))}. Follow the statements and sources behind this discussion.</p>
      <span class="card-foot"><span><span class="pill-row"><span class="pill">${fmt(c.accepted_excerpts)} excerpts</span><span class="pill">${fmt(c.shows)} shows</span></span><span class="explore">Explore the evidence →</span></span>${previewSeries(item)}</span>
    </button>`;
  }

  function renderBriefing() {
    navState("briefing");
    const shown = INDEX.briefing.filter(item => state.briefingBucket === "all" || researchBucket(item) === state.briefingBucket)
      .sort((a,b) => Number(researchBucket(a) === "watchlist") - Number(researchBucket(b) === "watchlist") || (b.pulse_vol || 0) - (a.pulse_vol || 0));
    app.innerHTML = `<section class="view">${snapshotNote()}
      <div class="home-head"><div><div class="eyebrow">Your research desk</div><h1>The research briefing.</h1><p class="lede">Explore the issues, compare what people say, and follow every excerpt to its source.</p></div></div>

      <form class="toolbar compact-search" id="brief-ask" role="search"><input class="search" id="brief-question" aria-label="Ask Signal Desk" placeholder="Find an issue: AI jobs, agents, open models…"><button class="primary" type="submit">Find evidence</button></form>
      <label class="mobile-bucket">Research view<select class="select" id="briefing-view">${BUCKETS.map(([value,label])=>`<option value="${value}" ${state.briefingBucket===value?"selected":""}>${label}</option>`).join("")}</select></label><div class="briefing-tabs briefing-sections" aria-label="Briefing sections">${BUCKETS.map(([value,label]) => `<button class="chip" data-bucket="${value}" aria-pressed="${state.briefingBucket===value}">${label}</button>`).join("")}</div>
      <div class="section-head"><h2>${esc(BUCKETS.find(x=>x[0]===state.briefingBucket)?.[1] || "Research overview")}</h2><p>${shown.length} issues · ordered by recent mentions, not importance</p></div>
      <div class="signal-grid">${shown.slice(0,6).map(signalCard).join("") || `<div class="empty">No issues match this view.</div>`}</div>
      ${shown.length>6 ? `<details class="more-research"><summary>Explore ${shown.length-6} more issues</summary><div class="signal-grid">${shown.slice(6).map(signalCard).join("")}</div></details>` : ""}${coverageFooter()}</section>`;
    byId("briefing-view").onchange=event=>{state.briefingBucket=event.target.value;renderBriefing();};
    document.querySelectorAll("[data-bucket]").forEach(button => button.onclick=()=>{state.briefingBucket=button.dataset.bucket;renderBriefing();});
    byId("brief-ask").onsubmit=event=>{event.preventDefault();const q=byId("brief-question").value.trim();if(q)route("ask",q);};
  }

  function issueCard(issue) {
    const brief = issue.brief;
    const c = brief.coverage || {};
    return `<button class="issue-card" data-route="issue" data-id="${esc(issue.id)}">
      <span class="issue-row"><span>
        <span class="row-top"><span class="row-title">${esc(cap(issue.name))}</span><span class="meta">${fmt(issue.pulse_vol)} recent</span></span>
        <span class="pill-row"><span class="pill ${brief.decision_grade ? "good" : "warn"}">Legacy evidence</span><span class="pill">${c.shows || 0} shows</span><span class="pill">${c.accepted_excerpts || 0} evidence</span></span>
        <span class="meta">Open statements, sources, and attention history.</span>
      </span>${previewSeries(issue)}</span>
    </button>`;
  }

  function issueSort(items) {
    const rows = [...items];
    if (state.issueSort === "rising") rows.sort((a, b) => (b.latest_share || 0) - (a.latest_share || 0));
    else if (state.issueSort === "breadth") rows.sort((a, b) => (b.pulse_shows || 0) - (a.pulse_shows || 0));
    else if (state.issueSort === "volume") rows.sort((a, b) => (b.pulse_vol || 0) - (a.pulse_vol || 0));
    else rows.sort((a, b) => Number(b.brief.decision_grade) - Number(a.brief.decision_grade) || (b.pulse_shows || 0) - (a.pulse_shows || 0));
    return rows;
  }

  function renderIssues() {
    navState("issues");
    const q = state.issueQuery.casefold?.() || state.issueQuery.toLowerCase();
    const aliases = INDEX.aliases.issues || {};
    const exact = aliases[q];
    let rows = INDEX.issues.filter(issue => {
      const matches = !q || issue.id === exact || [issue.name, ...(issue.aliases || [])].join(" ").toLowerCase().includes(q);
      const confidence = state.issueConfidence === "all" || issue.brief.confidence === state.issueConfidence;
      return matches && confidence;
    });
    rows = issueSort(rows);
    app.innerHTML = `<section class="view">${snapshotNote()}
      <div class="home-head"><div><div class="eyebrow">Strategic issues</div><h1>What is at stake?</h1>
        <p class="lede">Find a question, inspect the recorded statements, and trace the evidence. Compact charts show attention within this corpus.</p></div>
      </div>
      <div class="toolbar">
        <input class="search" id="issue-search" aria-label="Search issues and aliases" placeholder="Search issues and aliases" value="${esc(state.issueQuery)}">
        <select class="select" id="issue-sort" aria-label="Sort issues">
          <option value="strategic">Evidence breadth</option><option value="rising">Largest recent attention share</option>
          <option value="breadth">Broadest discussion</option><option value="volume">Most discussed</option>
        </select>
        <select class="select" id="issue-confidence" aria-label="Filter by confidence">
          <option value="all">All legacy coverage tiers</option><option value="high">High legacy coverage</option>
          <option value="medium">Medium legacy coverage</option><option value="watchlist">Watchlist</option>
        </select>
      </div>
      <div class="results-status" id="issue-status" role="status">${rows.length} issues</div>
      <div class="list" id="issue-results">${rows.length ? rows.map(issueCard).join("") : `<div class="empty">No issue matches. Try an alias, remove a confidence filter, or browse all issues.</div>`}</div>
      ${coverageFooter()}
    </section>`;
    byId("issue-sort").value = state.issueSort;
    byId("issue-confidence").value = state.issueConfidence;
    byId("issue-search").oninput = event => { const pos=event.target.selectionStart; state.issueQuery = event.target.value; renderIssues(); byId("issue-search")?.focus(); byId("issue-search")?.setSelectionRange(pos,pos); };
    byId("issue-sort").onchange = event => { state.issueSort = event.target.value; renderIssues(); };
    byId("issue-confidence").onchange = event => { state.issueConfidence = event.target.value; renderIssues(); };
  }

  function resolveIssue(value) {
    const key = String(value || "").toLowerCase();
    const id = INDEX.aliases.issues[key] || value;
    return id;
  }

  function resolveVoice(value) {
    const key = String(value || "").toLowerCase();
    return INDEX.aliases.voices[key] || value;
  }

  function trend(issue, activeMonth = "") {
    const series = issue.series || [];
    const visible = series.slice(-24);
    const values = visible.map(point => point.share_smooth ?? point.share ?? 0);
    const max = Math.max(...values, .0001);
    return `<div class="trend" role="group" aria-label="Monthly share of discourse">${visible.map((point, i) => {
      const share = values[i];
      const label = `${point.month}: ${point.vol || 0} mentions, ${pct(share)} of discourse${point.low_sample ? ", low coverage" : ""}`;
      return `<button class="month-bar ${point.low_sample ? "low" : ""} ${activeMonth === point.month ? "active" : ""}" data-month="${esc(point.month)}" aria-pressed="${activeMonth === point.month}" aria-label="${esc(label)}" title="${esc(label)}"><span style="height:${Math.max(2, Math.round(share / max * 132))}px"></span>${i % 4 === 0 ? `<span class="month-label">${esc(point.month)}</span>` : ""}</button>`;
    }).join("")}</div><p class="legend">Three-month smoothed share of discourse. Dim bars have low corpus coverage. Select a month to filter the evidence.</p>`;
  }

  function evidenceCard(evidence, issueId, options = {}) {
    const url=safeUrl(evidence.source_url), name=excerptIdentity(evidence);
    const personId=resolveVoice(evidence.person),hasPerson=name!=="Speaker not established"&&INDEX.voices.some(person=>person.id===personId);
    const origin=options.origin || "issue", originId=options.originId || issueId;
    return `<article class="evidence" id="ev-${esc(evidence.id)}"><div class="evidence-top"><div>
      ${hasPerson?`<button class="person-link" data-route="voice" data-id="${esc(personId)}">${esc(name)}</button>`:`<strong>${esc(name)}</strong>`}
      <div class="meta">${esc(evidence.show)} · ${esc(evidence.date)} · ${esc(cap(evidence.attribution_type || "attribution unavailable"))}</div>
      </div><span class="stance ${esc(evidence.group || "neutral")}">${esc(evidence.stance || "unknown")}</span></div>
      ${evidence.claim_text?`<p class="claim-summary">${esc(evidence.claim_text)}</p>`:""}
      <p class="quote">“${esc(evidence.evidence)}”</p>
      <div class="source-line"><span>${esc(evidence.episode)}</span><span class="source-actions">
      <button class="text-button" data-route="evidence" data-id="${esc(evidence.id)}" data-tail="${esc(origin)}/${esc(originId)}">Inspect excerpt</button>${url?`<a href="${esc(url)}" target="_blank" rel="noopener">Original source ↗</a>`:""}</span></div></article>`;
  }

  function relatedCards(issue) {
    const rows = (issue.related || []).filter(row => {
      const id = resolveIssue(row.topic);
      return INDEX.issues.some(candidate => candidate.id === id);
    });
    if (!rows.length) return `<div class="empty">No accepted relationship is available yet.</div>`;
    return rows.slice(0, 8).map(row => {
      const id = resolveIssue(row.topic);
      return `<button class="claim-card" data-route="issue" data-id="${esc(id)}"><b>${esc(cap(row.topic))}</b><p>${row.shared_episodes} co-mentioned episodes across ${row.shared_shows} shows</p></button>`;
    }).join("");
  }

  async function renderIssue(value, filterType="", filterValue="") {
    navState("issues");
    const expected=location.hash,id=resolveIssue(value),payload=await load("issues");
    if(location.hash!==expected)return;
    const issue=payload.issues[id];if(!issue)return renderNotFound("issues");
    state.selectedMonth=filterType==="month"?filterValue:"";
    const newest=(a,b)=>String(b.date || "").localeCompare(String(a.date || ""));
    const accepted=[...(issue.accepted_evidence || [])].sort(newest), uncertain=[...(issue.uncertain_evidence || [])].sort(newest);
    const slice=rows=>state.selectedMonth?rows.filter(e=>e.month===state.selectedMonth):rows;
    const selectedAccepted=slice(accepted),selectedUncertain=slice(uncertain);
    const evidence=state.evidenceMode==="accepted"?selectedAccepted:selectedUncertain;
    const named=evidence.filter(e=>excerptIdentity(e)!=="Speaker not established");
    const showCount=new Set(evidence.map(e=>e.show)).size;
    const positive=selectedAccepted.filter(e=>e.group==="positive"),negative=selectedAccepted.filter(e=>e.group==="negative");
    const published=INDEX.issues.some(i=>i.id===id)||INDEX.briefing.some(i=>i.id===id);
    if(!published)return renderNotFound("issues");
    app.innerHTML=`<section class="view">${snapshotNote()}${breadcrumb([{label:"Issues",route:"issues"},{label:cap(issue.name)}])}
      <div class="detail-head"><div><div class="eyebrow detail-label">Issue research · ${state.selectedMonth?esc(state.selectedMonth):"All available evidence"}</div><h1>${esc(cap(issue.name))}</h1><p class="lede">Explore the statements behind this issue, the sources they come from, and how attention changed over time.</p></div></div>
      <div class="scope-strip"><span><strong>${evidence.length}</strong> excerpts in this view</span><span><strong>${showCount}</strong> source shows</span><span><strong>${named.length}</strong> named excerpts</span>${state.selectedMonth?`<button class="text-button" data-route="issue" data-id="${esc(id)}">Clear month filter</button>`:""}</div>
      ${readingLinks([["issue-evidence","Statements"],["issue-history","Attention history"],["issue-positions","Stance comparison"],["issue-limits","Limitations"]])}
      ${!published?`<div class="callout">This candidate is outside the published research index. Its limited legacy evidence is shown for inspection, not as a supported conclusion.</div>`:""}
      <div class="research-grid"><div>
        <section class="panel" id="issue-evidence"><div class="section-head section-head-tight"><h2>What is being said</h2><p>${state.selectedMonth?esc(state.selectedMonth):"All available dates"}</p></div>
          <p class="panel-sub">Newest excerpts first. Attribution and source context are still under review; inspect the original material before relying on a claim.</p>
          <div class="filter-row"><button class="chip" id="accepted-toggle" aria-pressed="${state.evidenceMode==="accepted"}">Legacy accepted · ${selectedAccepted.length}</button><button class="chip" id="uncertain-toggle" aria-pressed="${state.evidenceMode==="uncertain"}">Uncertain · ${selectedUncertain.length}</button></div>
          <div class="evidence-list">${evidence.slice(0,3).map(e=>evidenceCard(e,id)).join("") || `<div class="empty">No excerpts match this slice. Select another month or clear the month filter.</div>`}</div>
          ${evidence.length>3?`<details class="evidence-more"><summary>Read ${evidence.length-3} more excerpts</summary><div class="evidence-list">${evidence.slice(3).map(e=>evidenceCard(e,id)).join("")}</div></details>`:""}</section>
        <section class="panel" id="issue-history"><h2 class="panel-title">How attention moved</h2><p class="panel-sub">Share of recorded discourse, not adoption or importance. Select a month to filter the evidence and stance comparison.</p><label class="month-picker">Evidence month<select class="select" id="issue-month"><option value="">All available months</option>${(issue.series || []).map(point=>`<option value="${esc(point.month)}" ${state.selectedMonth===point.month?"selected":""}>${esc(point.month)} · ${fmt(point.vol)} mentions</option>`).join("")}</select></label>${trend(issue,state.selectedMonth)}
          <details class="data-table"><summary>Read monthly data</summary><div class="table-scroll"><table><caption>Issue attention within the legacy corpus</caption><thead><tr><th>Month</th><th>Mentions</th><th>Share</th><th>Smoothed</th><th>Coverage</th></tr></thead><tbody>${(issue.series || []).map(p=>`<tr><th scope="row"><button class="text-button" data-month="${esc(p.month)}">${esc(p.month)}</button></th><td>${fmt(p.vol)}</td><td>${pct(p.share)}</td><td>${pct(p.share_smooth)}</td><td>${p.low_sample?"Low sample":"Recorded"}</td></tr>`).join("")}</tbody></table></div></details></section>
        <section class="panel" id="issue-positions"><h2 class="panel-title">How the excerpts are labeled</h2><p class="panel-sub">${state.selectedMonth?esc(state.selectedMonth):"Whole-issue sample"}. These stance labels are not adjudicated disagreements on the same proposition.</p>
          <div class="position-columns"><div><h3>Supportive · ${positive.length}</h3>${positive.slice(0,1).map(e=>evidenceCard(e,id)).join("")||`<p class="empty">No supportive excerpt in this slice.</p>`}</div><div><h3>Skeptical / warning · ${negative.length}</h3>${negative.slice(0,1).map(e=>evidenceCard(e,id)).join("")||`<p class="empty">No skeptical excerpt in this slice.</p>`}</div></div></section>
      </div><aside><section class="panel" id="issue-limits"><h2 class="panel-title">What this can establish</h2><p>The sample lets you inspect recorded discussion. It does not yet establish a reliable strategic conclusion or industry consensus.</p><ul class="watch-list"><li>${evidence.length-named.length} legacy excerpts in this slice lack a named speaker.</li><li>Source breadth is separate from expertise and correctness.</li><li>Implications and matched disagreements await approved claim-level synthesis.</li></ul><button class="text-button" data-route="coverage" data-tail="issue/${esc(id)}">Inspect source coverage</button></section>
        <section class="panel"><h2 class="panel-title">Keep exploring</h2><p class="panel-sub">Co-mentioned issues are research leads, not established relationships.</p><div class="claim-list">${relatedCards(issue)}</div></section></aside></div>${coverageFooter()}</section>`;
    byId("issue-month").onchange=event=>route("issue",id,event.target.value?["month",event.target.value]:[]);
    byId("accepted-toggle").onclick=()=>{state.evidenceMode="accepted";renderIssue(id,filterType,filterValue);};
    byId("uncertain-toggle").onclick=()=>{state.evidenceMode="uncertain";renderIssue(id,filterType,filterValue);};
    document.querySelectorAll("[data-month]").forEach(button=>button.onclick=()=>route("issue",id,["month",button.dataset.month]));
  }

  function voiceCard(person) {
    return `<button class="voice-card" data-route="voice" data-id="${esc(person.id)}"><span class="row-top"><span class="row-title">${esc(person.name)}</span><span class="meta">${fmt(person.n_episodes)} episodes</span></span><span class="pill-row"><span class="pill">${fmt(person.direct_evidence_count)} recorded direct excerpts</span><span class="pill">${person.shows.length} shows</span></span><span class="meta">Explore statements, their sources, and attribution limits.</span></button>`;
  }

  function voiceRows() {
    const q = state.voiceQuery.toLowerCase();
    let rows = INDEX.voices.filter(person => !q || [person.name, ...person.shows, ...person.top_topics.map(x => x.topic)].join(" ").toLowerCase().includes(q));
    if (state.voiceView === "changed") rows = rows.filter(person => person.moves_count).sort((a, b) => b.moves_count - a.moves_count);
    else if (state.voiceView === "contrarian") rows = rows.filter(person => person.contrarian_count).sort((a, b) => b.contrarian_count - a.contrarian_count);
    else if (state.voiceView === "evidence") rows.sort((a, b) => b.direct_evidence_count - a.direct_evidence_count);
    else if (state.voiceView === "rising") rows.sort((a, b) => (b.network_reach?.score || 0) - (a.network_reach?.score || 0));
    return rows;
  }

  function renderVoices() {
    navState("voices");
    const rows = voiceRows();
    const views = [["evidence", "Most direct evidence"], ["influential", "Published profiles"]];
    app.innerHTML = `<section class="view">${snapshotNote()}<div class="home-head"><div><div class="eyebrow">Recorded voices</div><h1>Who is saying what?</h1>
      <p class="lede">Inspect a person’s recorded statements and original sources. Frequency and network reach are not expertise.</p></div></div>
      <input class="search" id="voice-search" aria-label="Search voices, shows, and issues" placeholder="Search people, shows, or issues" value="${esc(state.voiceQuery)}">
      <div class="briefing-tabs">${views.map(([value, label]) => `<button class="chip" data-voice-view="${value}" aria-pressed="${state.voiceView === value}">${label}</button>`).join("")}</div>
      <div class="results-status" role="status">${rows.length} voices</div>
      <div class="list">${rows.length ? rows.map(voiceCard).join("") : `<div class="empty">No voice matches this view. Clear the search or select another view.</div>`}</div>
      ${coverageFooter()}</section>`;
    byId("voice-search").oninput = event => { const pos=event.target.selectionStart; state.voiceQuery = event.target.value; renderVoices(); byId("voice-search")?.focus(); byId("voice-search")?.setSelectionRange(pos,pos); };
    document.querySelectorAll("[data-voice-view]").forEach(button => button.onclick = () => { state.voiceView = button.dataset.voiceView; renderVoices(); });
  }

  async function renderVoice(value) {
    navState("voices");const expected=location.hash,id=resolveVoice(value),payload=await load("voices");
    if(location.hash!==expected)return;const person=payload.voices[id];if(!person)return renderNotFound("voices");
    if(!INDEX.voices.some(p=>p.id===id))return renderNotFound("voices");
    const direct=[...(person.direct_evidence || [])].sort((a,b)=>String(b.date || "").localeCompare(String(a.date || ""))),mentions=person.mentions || [];
    const renderEvidence=e=>evidenceCard(e,e.issue_id||resolveIssue(e.topic),{origin:"voice",originId:id});
    app.innerHTML=`<section class="view">${snapshotNote()}${breadcrumb([{label:"Voices",route:"voices"},{label:person.name}])}<div class="detail-head"><div><div class="eyebrow detail-label">Voice research</div><h1>${esc(person.name)}</h1><p class="lede">Recorded statements, their original sources, and the issues they touch.</p></div></div>
      <div class="scope-strip"><span><strong>${direct.length}</strong> direct-labeled excerpts</span><span><strong>${person.n_episodes}</strong> episodes</span><span><strong>${person.shows.length}</strong> shows</span></div>
      ${readingLinks([["voice-statements","Statements"],["voice-mentions","Third-party mentions"],["voice-basis","Evidence basis"]])}
      <div class="research-grid"><div><section class="panel" id="voice-statements"><h2 class="panel-title">What the record says</h2><p class="panel-sub">Legacy direct-speech labels require source-context verification. Reported speech inside an excerpt must not be mistaken for this person’s own position.</p><div class="evidence-list">${direct.slice(0,3).map(renderEvidence).join("")||`<div class="empty">No direct-labeled evidence is available.</div>`}</div>${direct.length>3?`<details class="evidence-more"><summary>Read ${direct.length-3} more statements</summary><div class="evidence-list">${direct.slice(3).map(renderEvidence).join("")}</div></details>`:""}</section>
      <section class="panel" id="voice-mentions"><h2 class="panel-title">What others say about them</h2><p class="panel-sub">Third-party mentions are not this person’s own claims.</p><div class="evidence-list">${mentions.slice(0,3).map(renderEvidence).join("")||`<div class="empty">No separate mentions are attached.</div>`}</div>${mentions.length>3?`<details class="evidence-more"><summary>Read ${mentions.length-3} more mentions</summary><div class="evidence-list">${mentions.slice(3).map(renderEvidence).join("")}</div></details>`:""}</section></div>
      <aside><section class="panel" id="voice-basis"><h2 class="panel-title">Evidence, not a reputation score</h2><p>A verified domain-specific expertise assessment is not available in this payload. The site does not rank this person’s correctness from popularity.</p><h3 class="subhead">Source appearances</h3><p>${esc(person.shows.join(" · "))}</p><button class="text-button" data-route="coverage" data-tail="voice/${esc(id)}">Inspect source coverage</button></section>
      <section class="panel"><h2 class="panel-title">Issues in the record</h2><div class="claim-list">${person.top_topics.slice(0,8).map(topic=>{const issueId=resolveIssue(topic.topic),exists=INDEX.issues.some(i=>i.id===issueId);return exists?`<button class="claim-card" data-route="issue" data-id="${esc(issueId)}"><b>${esc(cap(topic.topic))}</b><p>Open issue evidence →</p></button>`:`<div class="topic-label">${esc(cap(topic.topic))}<small>Canonical issue page not yet available</small></div>`;}).join("")}</div></section></aside></div>${coverageFooter()}</section>`;
  }

  function scoreTokens(tokens, text) {
    const haystack = String(text || "").toLowerCase();
    return tokens.reduce((score, token) => score + (haystack.includes(token) ? 1 : 0), 0) / Math.max(tokens.length, 1);
  }

  const STOP = new Set(["the","and","for","what","who","how","does","about","with","that","this","are","was","have","has","from","will","would","should","could","why","when","where","which","their","there","been","being","them","they","into","than","then","some","any","all","can","say","says","said","most","more","less","just","like"]);
  function queryTokens(q) {
    return String(q || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").split(/\s+/).filter(token => token.length > 2 && !STOP.has(token));
  }

  function askMatches(q) {
    const tokens = queryTokens(q);
    const exact = INDEX.aliases.issues[String(q || "").toLowerCase()];
    return INDEX.issues.map(issue => ({
      issue,
      score: issue.id === exact ? 2 : scoreTokens(tokens, [issue.name, ...(issue.aliases || []), issue.brief.what_changed.text, issue.brief.why_it_matters.text].join(" ")),
    })).filter(row => row.score >= .34).sort((a, b) => b.score - a.score || Number(b.issue.brief.decision_grade) - Number(a.issue.brief.decision_grade));
  }

  function renderAsk(q = "") {
    navState("ask");
    const matches = q ? askMatches(q) : [];
    const best = matches[0]?.issue;
    app.innerHTML = `<section class="view">${snapshotNote()}<div class="home-head"><div><div class="eyebrow">Ask · issue lookup</div><h1>Ask the evidence.</h1>
      <p class="lede">Find the issues related to your question, then explore their evidence. A generated answer requires approved claim-level synthesis, which is not available yet.</p></div></div>
      <form class="toolbar" id="ask-form" role="search" style="grid-template-columns:minmax(0,1fr) auto">
        <input class="search" id="ask-input" aria-label="Ask Signal Desk" value="${esc(q)}" placeholder="What changed on AI jobs? Where do credible voices disagree on agents?">
        <button class="primary" type="submit">Ask</button>
      </form>
      ${!q ? `<div class="empty">Ask about a tracked industry issue. You will get matching research paths and their evidence limitations.</div>` :
        !best ? `<div class="callout danger">Signal Desk cannot confidently resolve that question to a tracked issue. Try a specific issue, product, company, or policy term.</div>` :
        `<section class="panel"><div class="eyebrow">Resolved to ${esc(best.name)}</div><h2 class="panel-title">${esc(cap(best.name))}</h2><p class="panel-sub">This is an issue match, not an answer to your question. Read the statements and source material to assess it.</p>
          <div class="button-row"><button class="primary" data-route="issue" data-id="${esc(best.id)}">Explore issue evidence</button>${coveragePill(best.brief, best.id)}</div>
          ${matches.length > 1 ? `<div class="section-head"><h3>Other possible meanings</h3></div><div class="claim-list">${matches.slice(1, 5).map(row => `<button class="claim-card" data-route="issue" data-id="${esc(row.issue.id)}"><b>${esc(row.issue.name)}</b><p>${esc(row.issue.brief.what_changed.text)}</p></button>`).join("")}</div>` : ""}
        </section>`}
      ${coverageFooter()}</section>`;
    byId("ask-form").onsubmit = event => {
      event.preventDefault();
      const value = byId("ask-input").value.trim();
      route("ask", value);
    };
  }

  function stageLoss(current, previous, key) {
    if (!previous) return 0;
    return Math.max(0, Number(previous[key] || 0) - Number(current[key] || 0));
  }

  function stageCard(stage,index,stages) {
    const previous=stages[index-1],dims=["shows","episodes","segments"];
    return `<article class="stage-card"><div class="stage-head"><div><span class="eyebrow">Stage ${index+1}</span><h3>${esc(stage.label)}</h3></div></div><div class="stage-metrics">${dims.map(key=>`<span><b>${fmt(stage[key])}</b><small>${key}${key==="segments"&&index<2?" · not created yet":""}</small></span>`).join("")}</div><div class="conversion-grid">${dims.map(key=>{
      const prior=Number(previous?.[key] || 0),current=Number(stage[key] || 0);
      if(!previous)return `<span class="conversion">${key}: ${key==="segments"?"not applicable":"starting cohort"}</span>`;
      if(key==="segments"&&!prior)return `<span class="conversion">segments: ${current?"created at this stage":"not applicable"}</span>`;
      if(!prior)return `<span class="conversion">${key}: no prior denominator</span>`;
      return `<span class="conversion">${key}: ${Math.round(current/prior*100)}% retained · ${fmt(stageLoss(stage,previous,key))} fewer</span>`;
    }).join("")}</div></article>`;
  }

  function showCard(show) {
    const url = safeUrl(show.rss_url);
    return `<article class="show-card"><div class="show-card-top"><div><h3>${esc(show.name)}</h3>
      <div class="pill-row"><span class="pill">${esc(cap(show.category || "uncategorized"))}</span><span class="pill ${show.coverage_percent >= 80 ? "good" : "warn"}">${show.coverage_percent}% ready</span>${show.duplicate_source ? `<span class="pill warn">Duplicate source</span>` : ""}${show.transcript_quarantined ? `<span class="pill shift">${show.transcript_quarantined} quarantined</span>` : ""}</div></div>
      ${url ? `<a href="${esc(url)}" target="_blank" rel="noopener" aria-label="Open RSS feed for ${esc(show.name)}">Open RSS</a>` : ""}</div>
      <div class="show-metrics"><span><b>${fmt(show.catalogued_episodes)}</b>catalogued</span><span><b>${fmt(show.transcript_attempts)}</b>attempted</span><span><b>${fmt(show.intelligence_ready_episodes)}</b>ready</span><span><b>${fmt(show.missing_episodes)}</b>missing</span></div>
      ${show.aliases?.length ? `<p class="meta" style="margin-top:8px">Aliases: ${esc(show.aliases.join(" · "))}</p>` : ""}
    </article>`;
  }

  function enrollmentBody() {
    const name = byId("enroll-name").value.trim();
    const rss = byId("enroll-rss").value.trim();
    const category = byId("enroll-category").value;
    return {name, rss, category, body: [
      "## Podcast enrollment request", "", `- Show: ${name}`, `- RSS feed: ${rss}`,
      `- Category: ${category}`, "- Policy: private_analysis_only",
      "- Transcript policy: creator_rss_or_official_public_transcripts", "",
      "## Preflight", "", "- [ ] Resolve and canonicalize the feed URL",
      "- [ ] Check existing show IDs and aliases", "- [ ] Preview canonical title and latest episode",
      "- [ ] Run bounded ingestion dry run before enqueueing work",
    ].join("\n")};
  }

  async function renderCoverage(filterType = "", filterValue = "") {
    navState("coverage");
    const expected=location.hash;
    const draft=byId("enroll-name") ? {name:byId("enroll-name").value,rss:byId("enroll-rss").value,category:byId("enroll-category").value} : (state.enrollmentDraft || null);
    if(draft)state.enrollmentDraft=draft;
    const caret=byId("coverage-search")?.selectionStart;
    const payload = await load("coverage");
    if(location.hash!==expected)return;
    const funnel = payload.funnel;
    let rows = [...funnel.shows];
    if (state.coverageGap === "needs_attention") rows = rows.filter(show => show.missing_episodes || show.transcript_quarantined || show.duplicate_source);
    else if (state.coverageGap === "duplicate") rows = rows.filter(show => show.duplicate_source);
    else if (state.coverageGap === "quarantine") rows = rows.filter(show => show.transcript_quarantined);
    if (state.coverageQuery) rows = rows.filter(show => [show.name, ...(show.aliases || []), show.category].join(" ").toLowerCase().includes(state.coverageQuery.toLowerCase()));
    const first = funnel.stages[0] || {};
    const attempted = funnel.stages[1] || {};
    const missingAttempts = Math.max(0, (first.episodes || 0) - (attempted.episodes || 0));
    const duplicateCount = funnel.duplicate_source_groups?.length || 0;
    app.innerHTML = `<section class="view">${snapshotNote()}
      <div class="home-head"><div><div class="eyebrow">Coverage &amp; Trust</div><h1>Where are the blind spots?</h1>
        <p class="lede">Understand what entered the corpus, what was lost, which sources need attention, and how coverage can bias a finding.</p></div>
        <div class="status">Server snapshot · ${esc(new Date(payload.generated_at).toLocaleString())}</div></div>
      ${filterType ? `<div class="callout scope-notice">You arrived from ${esc(cap(filterType))} research. This payload contains global coverage; an exact issue/voice-to-source coverage map is not yet published.</div>` : ""}
      <div class="callout ${missingAttempts || duplicateCount ? "danger" : ""}"><strong>Largest current risk:</strong> ${fmt(missingAttempts)} catalogued episodes have not reached transcript attempt. ${duplicateCount ? `${duplicateCount} duplicate feed group requires adjudication.` : "No duplicate feed is detected."}</div>
      <div class="coverage-grid">${funnel.stages.map(stageCard).join("")}</div><p class="legend">Stage totals use the producer’s recorded cohorts. The roster resolves ${funnel.raw_show_count} raw feed records to ${funnel.canonical_show_count} canonical sources. Stage-specific loss membership is not supplied by this snapshot.</p>
      <div class="research-grid coverage-layout">
        <section class="panel coverage-roster"><div class="section-head" style="margin-top:0"><div><div class="eyebrow">Canonical sources</div><h2>${state.coverageGap === "all" ? "All sources" : state.coverageGap === "duplicate" ? "Duplicate feeds" : state.coverageGap === "quarantine" ? "Quarantined sources" : "Needs attention"}</h2></div><p>${funnel.canonical_show_count} canonical · ${funnel.raw_show_count} raw records</p></div>
          <div class="toolbar" style="grid-template-columns:minmax(0,1fr) 210px">
            <input class="search" id="coverage-search" aria-label="Search coverage sources" placeholder="Search shows or categories" value="${esc(state.coverageQuery)}">
            <select class="select" id="coverage-gap" aria-label="Filter coverage gaps">
              <option value="needs_attention">Needs attention</option><option value="all">All sources</option>
              <option value="duplicate">Duplicate feeds</option><option value="quarantine">Quarantined</option>
            </select>
          </div>
          <div class="results-status" role="status">${rows.length} sources</div>
          <div class="show-list">${rows.length ? rows.map(showCard).join("") : `<div class="empty">No source matches this coverage view.</div>`}</div>
        </section>
        <aside class="source-intake"><section class="panel"><div class="eyebrow">Source intake</div><h2 class="panel-title">Add a new show</h2>
          <p class="panel-sub">Prepare an enrollment request to copy or open in your mail app. This form does not enroll a show or send a request automatically.</p>
          <form class="enroll-form" id="enroll-form" novalidate>
            <div class="form-field"><label for="enroll-name">Show name</label><input class="search" id="enroll-name" required autocomplete="off" placeholder="Podcast name"><div class="form-error" id="name-error"></div></div>
            <div class="form-field"><label for="enroll-rss">RSS feed URL</label><input class="search" id="enroll-rss" required type="url" inputmode="url" placeholder="https://example.com/feed.xml"><div class="form-error" id="rss-error"></div></div>
            <div class="form-field"><label for="enroll-category">Category</label><select class="select" id="enroll-category"><option value="frontier_ai">Frontier AI</option><option value="enterprise_ai">Enterprise AI</option><option value="developer_tools">Developer tools</option><option value="policy_governance">Policy &amp; governance</option><option value="business_markets">Business &amp; markets</option><option value="general_technology">General technology</option></select></div>
            <div class="form-actions"><button class="primary" type="submit">Open email draft</button><button class="secondary" id="copy-enroll" type="button">Copy enrollment request</button></div>
            <div class="results-status" id="enroll-status" role="status" aria-live="polite"></div>
          </form>
        </section>
        <section class="panel"><h2 class="panel-title">Metric reconciliation</h2><p class="panel-sub">Bars no longer use episode conversion to imply show or segment conversion. Every dimension is shown separately.</p>
          <p><strong>${fmt(funnel.quarantined_transcript_episodes)}</strong> quarantined transcript attempts remain outside intelligence-ready evidence.</p>
          ${funnel.duplicate_source_groups?.length ? `<div class="callout danger" style="margin-top:12px">${funnel.duplicate_source_groups.map(group => `${esc(group.names.join(" / "))} share one feed`).join("<br>")}</div>` : ""}
        </section></aside>
      </div>${coverageFooter()}</section>`;
    byId("coverage-gap").value = state.coverageGap;
    if(draft){byId("enroll-name").value=draft.name;byId("enroll-rss").value=draft.rss;byId("enroll-category").value=draft.category;}
    if(caret!=null && state.coverageQuery){byId("coverage-search").focus();byId("coverage-search").setSelectionRange(caret,caret);}
    byId("coverage-search").oninput = event => { state.coverageQuery = event.target.value; renderCoverage(filterType, filterValue); byId("coverage-search")?.focus(); };
    byId("coverage-gap").onchange = event => { state.coverageGap = event.target.value; renderCoverage(filterType, filterValue); };
    const validate = () => {
      const request = enrollmentBody();
      let ok = true;
      byId("name-error").textContent = "";
      byId("rss-error").textContent = "";
      if (!request.name) byId("name-error").textContent = "Add a show name.", ok = false;
      try {
        const parsed = new URL(request.rss);
        if (!["http:", "https:"].includes(parsed.protocol)) throw new Error();
      } catch (_) {
        byId("rss-error").textContent = "Enter a complete http or https RSS URL.";
        ok = false;
      }
      const duplicate = funnel.shows.find(show => show.rss_url === request.rss || show.name.toLowerCase() === request.name.toLowerCase());
      if (duplicate) byId("rss-error").textContent = `Possible existing source: ${duplicate.name}. Review its aliases before enrollment.`, ok = false;
      return ok ? request : null;
    };
    byId("copy-enroll").onclick = async () => {
      const request = validate();
      if (!request) return;
      await navigator.clipboard.writeText(request.body);
      byId("enroll-status").textContent = "Private enrollment request copied.";
    };
    byId("enroll-form").onsubmit = event => {
      event.preventDefault();
      const request = validate();
      if (!request) return;
      const body = encodeURIComponent(request.body);
      location.href = `mailto:?subject=${encodeURIComponent(`Enroll podcast: ${request.name}`)}&body=${body}`;
      byId("enroll-status").textContent = "Email draft opened. Nothing has been sent or enrolled.";
    };
  }

  async function findEvidence(id) {
    const [issuesPayload, voicesPayload] = await Promise.all([load("issues"), load("voices")]);
    for (const [issueId, issue] of Object.entries(issuesPayload.issues)) {
      for (const evidence of [...issue.accepted_evidence, ...issue.uncertain_evidence]) {
        if (evidence.id === id) return {issueId, issue, evidence};
      }
    }
    for (const person of Object.values(voicesPayload.voices)) {
      for (const evidence of [...person.direct_evidence, ...person.mentions]) {
        if (evidence.id === id) {
          const issueId = evidence.issue_id || resolveIssue(evidence.topic);
          return {issueId, issue: issuesPayload.issues[issueId], evidence};
        }
      }
    }
    return null;
  }

  async function renderEvidence(id,filterType="",filterValue="") {
    navState("");const expected=location.hash,hit=await findEvidence(id);if(location.hash!==expected)return;
    if(!hit)return renderNotFound(filterType || "briefing");
    const {issueId,issue,evidence}=hit,originRoute=filterType==="voice"?"voice":"issue",originId=filterValue||issueId,url=safeUrl(evidence.source_url);
    const hasContext=Boolean(evidence.context_before || evidence.context_after);
    app.innerHTML=`<section class="view evidence-page">${snapshotNote()}${breadcrumb([{label:originRoute==="voice"?"Voice profile":"Issue research",route:originRoute,id:originId},{label:"Evidence"}])}
      <div class="eyebrow detail-label">Source evidence · legacy record</div><h1>${esc(cap(issue?.name || evidence.topic || "Evidence"))}</h1><p class="lede">${esc(excerptIdentity(evidence))} · ${esc(evidence.show)} · ${esc(evidence.date)}</p>
      <p class="source-title">${esc(evidence.episode)}</p>${evidence.claim_text?`<section class="panel"><h2 class="panel-title">Extracted claim</h2><p>${esc(evidence.claim_text)}</p></section>`:""}
      <section class="panel"><h2 class="panel-title">${hasContext?"Excerpt in context":"Recorded excerpt"}</h2><div class="context-quote">${evidence.context_before?`<span class="context-dim">${esc(evidence.context_before)} </span>`:""}<mark>${esc(evidence.evidence)}</mark>${evidence.context_after?`<span class="context-dim"> ${esc(evidence.context_after)}</span>`:""}</div>
      ${!hasContext?`<div class="callout">Surrounding context is not included in this public record. Open the original source to verify the statement; this fragment alone may not establish its meaning.</div>`:""}<div class="button-row">${url?`<a class="primary" href="${esc(url)}" target="_blank" rel="noopener">Open original source ↗</a>`:`<p class="empty">Original source unavailable.</p>`}<button class="secondary" id="research-return" data-route="${originRoute}" data-id="${esc(originId)}">Return to ${originRoute==="voice"?"voice":"issue"}</button></div></section>
      <details class="panel provenance"><summary>Inspect provenance and limitations</summary><dl><dt>Attribution label</dt><dd>${esc(cap(evidence.attribution_type || "unavailable"))}</dd><dt>Legacy publication label</dt><dd>${esc(cap(evidence.publishability || "unavailable"))}</dd><dt>Recorded stance</dt><dd>${esc(evidence.stance || "unknown")}</dd><dt>Evidence ID</dt><dd>${esc(evidence.id)}</dd></dl><p>These legacy labels are not a clean-corpus approval receipt or a probability that the claim is true.</p>${evidence.quality_reasons?.length?`<p>${esc(evidence.quality_reasons.join(" · "))}</p>`:""}</details>${coverageFooter()}</section>`;
    byId("research-return").onclick=event=>{if(history.state?.origin){event.preventDefault();event.stopPropagation();history.back();}};
  }

  function renderNotFound(returnRoute = "briefing") {
    navState("");
    app.innerHTML = `<section class="view"><div class="eyebrow">Unavailable</div><h1>That view moved.</h1><p class="lede">The nightly registry changed or the record is no longer publishable. Your research path is preserved where possible.</p><div class="button-row"><button class="primary" data-route="${esc(returnRoute)}">Return to ${esc(cap(returnRoute))}</button></div></section>`;
  }

  function renderError(error) {
    app.innerHTML = `<section class="view"><div class="eyebrow">Data unavailable</div><h1>The briefing could not load.</h1><p class="lede">${esc(error.message || error)}</p><div class="button-row"><button class="primary" id="retry">Try again</button></div></section>`;
    byId("retry").onclick = () => { cache.clear(); boot(); };
  }

  async function render() {
    try {
      if (!INDEX) INDEX = await load("index");
      const expected=location.hash;
      if(history.state?.filters)Object.assign(state,history.state.filters);
      const current = parseRoute();
      if (current.kind === "briefing") renderBriefing();
      else if (current.kind === "issues") renderIssues();
      else if (current.kind === "issue") await renderIssue(current.id, current.filterType, current.filterValue);
      else if (current.kind === "voices") renderVoices();
      else if (current.kind === "voice") await renderVoice(current.id);
      else if (current.kind === "ask") renderAsk(current.id);
      else if (current.kind === "coverage") await renderCoverage(current.filterType, current.filterValue);
      else if (current.kind === "evidence") await renderEvidence(current.id, current.filterType, current.filterValue);
      else renderNotFound();
      if(location.hash!==expected)return;
      app.focus({preventScroll:true});
      const scroll=history.state?.scroll || 0;
      requestAnimationFrame(()=>{if(location.hash!==expected)return;app.scrollTop=scroll;window.scrollTo(0,scroll);});
    } catch (error) {
      console.error(error);
      renderError(error);
    }
  }

  document.addEventListener("click", event => {
    const jump=event.target.closest("[data-jump]");
    if(jump){byId(jump.dataset.jump)?.scrollIntoView({behavior:"smooth",block:"start"});return;}
    const target = event.target.closest("[data-route]");
    if (!target || target.disabled) return;
    event.preventDefault();
    const tail = (target.dataset.tail || "").split("/").filter(Boolean);
    route(target.dataset.route, target.dataset.id || "", tail);
  });
  window.addEventListener("hashchange", render);
  window.addEventListener("popstate", render);
  window.setInterval(() => {
    if (parseRoute().kind !== "coverage") return;
    cache.delete("coverage");
    load("coverage").then(payload=>{
      const status=document.querySelector(".home-head .status");
      if(status && parseRoute().kind==="coverage")status.textContent=`Server snapshot · ${payload.generated_at}. Reopen Coverage to load updated counts.`;
    }).catch(()=>{});
  }, 60000);
  async function boot() {
    try {
      INDEX = await load("index");
      if (!location.hash) location.hash = "briefing";
      else await render();
    } catch (error) { renderError(error); }
  }
  boot();
})();
