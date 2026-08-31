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
    voiceView: "influential",
    evidenceMode: "accepted",
    selectedMonth: "",
    coverageGap: "needs_attention",
    coverageQuery: "",
  };
  let INDEX = null;

  const esc = value => String(value ?? "").replace(/[&<>"]/g, char => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;",
  }[char]));
  const cap = value => String(value ?? "").replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
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
    location.hash = parts.join("/");
  }

  function parseRoute() {
    const parts = (location.hash.slice(1) || "briefing").split("/").map(decodeURIComponent);
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
    return `<footer class="coverage-footer">Signal Desk publishes bounded excerpts, not transcripts. Confidence describes evidence coverage inside this corpus—not truth, probability, or investment advice. <button class="text-button" data-route="coverage">Inspect corpus coverage</button></footer>`;
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
    const tone = brief.decision_grade ? "good" : "warn";
    return `<button class="coverage-badge ${tone}" data-route="coverage" data-tail="issue/${esc(issueId)}" aria-label="Inspect coverage: ${c.accepted_excerpts || 0} accepted excerpts across ${c.shows || 0} shows">${brief.decision_grade ? "Decision-grade coverage" : "Watchlist coverage"} · ${c.shows || 0} shows</button>`;
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
    ["all", "All signals"], ["new", "New"], ["accelerating", "Accelerating"],
    ["changing_consensus", "Changing consensus"], ["fading", "Fading"],
    ["watchlist", "Watchlist"],
  ];

  function signalCard(item) {
    const brief = item.brief;
    const c = brief.coverage || {};
    return `<button class="signal-card ${brief.decision_grade ? "" : "watchlist"}" data-route="issue" data-id="${esc(item.id)}">
      <span class="signal-kind">${esc(cap(item.bucket))} · ${esc(brief.confidence)} confidence</span>
      <h3>${esc(item.name)}</h3>
      <span>
        <p>${esc(brief.what_changed.text)}</p>
        <p class="why"><strong>Why it matters:</strong> ${esc(brief.why_it_matters.text)}</p>
      </span>
      <span class="card-foot">
        <span><span class="pill-row"><span class="pill ${brief.decision_grade ? "good" : "warn"}">${c.accepted_excerpts || 0} accepted excerpts</span><span class="pill">${c.shows || 0} shows</span></span><span class="explore">Open decision brief →</span></span>
        ${spark(item.series_preview, brief.decision_grade ? "var(--green)" : "var(--gold)")}
      </span>
    </button>`;
  }

  function renderBriefing() {
    navState("briefing");
    const shown = INDEX.briefing.filter(item =>
      state.briefingBucket === "all" || item.bucket === state.briefingBucket);
    const decisionGrade = INDEX.briefing.filter(item => item.brief.decision_grade).length;
    app.innerHTML = `<section class="view">
      <div class="home-head">
        <div><div class="eyebrow">Evidence-grounded briefing</div><h1>What changed?</h1>
          <p class="lede">Start with movements that have real source breadth, then open the arguments, credible voices, implications, and evidence behind them.</p>
        </div>
        <div class="status">${decisionGrade} decision-grade · ${INDEX.briefing.length - decisionGrade} watchlist</div>
      </div>
      <form class="toolbar" id="brief-ask" role="search" style="grid-template-columns:minmax(0,1fr) auto">
        <input class="search" id="brief-question" aria-label="Ask Signal Desk" placeholder="Ask: what changed on AI jobs? Where do credible voices disagree?">
        <button class="primary" type="submit">Ask Signal Desk</button>
      </form>
      <div class="briefing-tabs" aria-label="Briefing sections">${BUCKETS.map(([value, label]) =>
        `<button class="chip" data-bucket="${value}" aria-pressed="${state.briefingBucket === value}">${label}</button>`
      ).join("")}</div>
      <div class="section-head"><h2>${esc(BUCKETS.find(x => x[0] === state.briefingBucket)?.[1] || "Signals")}</h2><p>Ranked by evidence quality, source breadth, and movement</p></div>
      <div class="signal-grid">${shown.length ? shown.slice(0, 16).map(signalCard).join("") : `<div class="empty">No signals meet this view’s evidence threshold.</div>`}</div>
      ${coverageFooter()}
    </section>`;
    document.querySelectorAll("[data-bucket]").forEach(button => button.onclick = () => {
      state.briefingBucket = button.dataset.bucket;
      renderBriefing();
    });
    byId("brief-ask").onsubmit = event => {
      event.preventDefault();
      const q = byId("brief-question").value.trim();
      if (q) route("ask", q);
    };
  }

  function issueCard(issue) {
    const brief = issue.brief;
    const c = brief.coverage || {};
    return `<button class="issue-card" data-route="issue" data-id="${esc(issue.id)}">
      <span class="issue-row"><span>
        <span class="row-top"><span class="row-title">${esc(issue.name)}</span><span class="meta">${fmt(issue.pulse_vol)} recent</span></span>
        <span class="pill-row"><span class="pill ${brief.decision_grade ? "good" : "warn"}">${esc(brief.confidence)} confidence</span><span class="pill">${c.shows || 0} shows</span><span class="pill">${c.accepted_excerpts || 0} evidence</span></span>
        <span class="meta">${esc(brief.what_changed.text)}</span>
      </span>${spark(issue.series_preview, brief.decision_grade ? "var(--green)" : "var(--gold)")}</span>
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
    app.innerHTML = `<section class="view">
      <div class="home-head"><div><div class="eyebrow">Strategic issues</div><h1>What is at stake?</h1>
        <p class="lede">Find the live questions, strongest arguments, credible voices, and evidence limitations behind industry change.</p></div>
      </div>
      <div class="toolbar">
        <input class="search" id="issue-search" aria-label="Search issues and aliases" placeholder="Search issues and aliases" value="${esc(state.issueQuery)}">
        <select class="select" id="issue-sort" aria-label="Sort issues">
          <option value="strategic">Strategic priority</option><option value="rising">Fastest-rising share</option>
          <option value="breadth">Broadest discussion</option><option value="volume">Most discussed</option>
        </select>
        <select class="select" id="issue-confidence" aria-label="Filter by confidence">
          <option value="all">All confidence</option><option value="high">High confidence</option>
          <option value="medium">Medium confidence</option><option value="watchlist">Watchlist</option>
        </select>
      </div>
      <div class="results-status" id="issue-status" role="status">${rows.length} issues</div>
      <div class="list" id="issue-results">${rows.length ? rows.slice(0, 80).map(issueCard).join("") : `<div class="empty">No issue matches. Try an alias, remove a confidence filter, or browse all issues.</div>`}</div>
      ${coverageFooter()}
    </section>`;
    byId("issue-sort").value = state.issueSort;
    byId("issue-confidence").value = state.issueConfidence;
    byId("issue-search").oninput = event => { state.issueQuery = event.target.value; renderIssues(); byId("issue-search")?.focus(); };
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
    const url = safeUrl(evidence.source_url);
    const personId = resolveVoice(evidence.person);
    const hasPerson = INDEX.voices.some(person => person.id === personId);
    return `<article class="evidence" id="ev-${esc(evidence.id)}">
      <div class="evidence-top"><div>
        ${hasPerson ? `<button class="person-link" data-route="voice" data-id="${esc(personId)}">${esc(evidence.person)}</button>` : `<strong>${esc(evidence.person || "Unattributed voice")}</strong>`}
        <div class="meta">${esc(cap(evidence.attribution_type || "uncertain attribution"))} · quality ${Math.round((evidence.quality_score || 0) * 100)} · ${esc(evidence.show)} · ${esc(evidence.date)}</div>
      </div><span class="stance ${esc(evidence.group || "neutral")}">${esc(evidence.stance || evidence.group || "neutral")}</span></div>
      <p class="quote">“${esc(evidence.evidence)}”</p>
      <div class="source-line"><span>${esc(evidence.episode)}</span><span class="source-actions">
        ${options.compact ? "" : `<button class="text-button" data-route="evidence" data-id="${esc(evidence.id)}" data-tail="issue/${esc(issueId)}">Read context</button>`}
        ${url ? `<a href="${esc(url)}" target="_blank" rel="noopener">Original source</a>` : ""}
      </span></div>
    </article>`;
  }

  function relatedCards(issue) {
    const rows = (issue.related || []).filter(row => INDEX.aliases.issues[String(row.topic || "").toLowerCase()]);
    if (!rows.length) return `<div class="empty">No accepted relationship is available yet.</div>`;
    return rows.slice(0, 8).map(row => {
      const id = resolveIssue(row.topic);
      return `<button class="claim-card" data-route="issue" data-id="${esc(id)}"><b>${esc(cap(row.topic))}</b><p>${row.shared_episodes} co-mentioned episodes across ${row.shared_shows} shows</p></button>`;
    }).join("");
  }

  async function renderIssue(value, filterType = "", filterValue = "") {
    navState("issues");
    const id = resolveIssue(value);
    const payload = await load("issues");
    const issue = payload.issues[id];
    if (!issue) return renderNotFound("issues");
    state.selectedMonth = filterType === "month" ? filterValue : "";
    const brief = issue.brief;
    let evidence = state.evidenceMode === "accepted" ? issue.accepted_evidence : issue.uncertain_evidence;
    if (state.selectedMonth) evidence = evidence.filter(item => item.month === state.selectedMonth);
    const positive = issue.accepted_evidence.filter(item => item.group === "positive").slice(0, 2);
    const negative = issue.accepted_evidence.filter(item => item.group === "negative").slice(0, 2);
    app.innerHTML = `<section class="view">
      ${breadcrumb([{label: "Issues", route: "issues"}, {label: issue.name}])}
      <div class="detail-head"><div><div class="eyebrow detail-label">${esc(cap(brief.classification))} · ${esc(brief.confidence)} confidence</div>
        <h1>${esc(issue.name)}</h1><p class="lede">${esc(brief.what_changed.text)}</p></div>${coveragePill(brief, id)}</div>
      <div class="metric-grid">
        <div class="metric"><b>${fmt(brief.coverage.accepted_excerpts)}</b><span>accepted excerpts</span></div>
        <div class="metric"><b>${fmt(brief.coverage.episodes)}</b><span>supporting episodes</span></div>
        <div class="metric"><b>${fmt(brief.coverage.shows)}</b><span>independent shows</span></div>
        <div class="metric"><b>${fmt(brief.coverage.voices)}</b><span>distinct voices</span></div>
      </div>
      <div class="research-grid mobile-priority">
        <div>
          <section class="panel brief-panel"><div class="eyebrow">Two-minute brief</div><h2 class="panel-title">Why it matters</h2>
            <div class="brief-stack">${sentence(brief.why_it_matters)}${brief.implications.map(item => sentence(item)).join("")}</div>
            <h3 style="margin-top:20px">What to watch</h3><ul class="watch-list">${brief.watchpoints.map(item => `<li>${esc(item)}</li>`).join("")}</ul>
          </section>
          <section class="panel evidence-panel"><div class="section-head" style="margin-top:0"><h2>Best evidence</h2><p>Direct, source-grounded excerpts</p></div>
            <div class="filter-row"><button class="chip" id="accepted-toggle" aria-pressed="${state.evidenceMode === "accepted"}">Accepted ${issue.accepted_evidence.length}</button>
              <button class="chip" id="uncertain-toggle" aria-pressed="${state.evidenceMode === "uncertain"}">Uncertain ${issue.uncertain_evidence.length}</button>
            </div>
            <div class="evidence-list" style="margin-top:14px">${evidence.length ? evidence.map(item => evidenceCard(item, id)).join("") : `<div class="empty">No ${state.evidenceMode} evidence matches this slice.</div>`}</div>
          </section>
          <section class="panel trend-panel"><h2 class="panel-title">How attention moved</h2>${trend(issue, state.selectedMonth)}</section>
          <section class="panel argument-panel"><h2 class="panel-title">Where the evidence differs</h2><p class="panel-sub">Claim-level stance from accepted excerpts; this is not a vote or prediction.</p>
            <div class="argument-row"><span class="argument-label">Supportive</span><div>${positive.length ? positive.map(item => evidenceCard(item, id, {compact: true})).join("") : `<div class="empty">No accepted supportive evidence.</div>`}</div></div>
            <div class="argument-row"><span class="argument-label">Skeptical</span><div>${negative.length ? negative.map(item => evidenceCard(item, id, {compact: true})).join("") : `<div class="empty">No accepted skeptical evidence.</div>`}</div></div>
          </section>
        </div>
        <aside>
          <section class="panel related-panel"><h2 class="panel-title">Research leads</h2><p class="panel-sub">Co-mentioned issues are leads, not causal relationships. Typed relationships remain hidden until adjudicated.</p><div class="claim-list">${relatedCards(issue)}</div></section>
          <section class="panel"><h2 class="panel-title">Evidence limitations</h2>
            ${brief.decision_grade ? `<div class="callout">This issue passes the publication threshold. Coverage still reflects this podcast corpus, not the whole industry.</div>` : `<div class="callout danger">Watchlist only: the signal does not yet meet the independent episode, show, voice, and excerpt threshold.</div>`}
          </section>
        </aside>
      </div>${coverageFooter()}
    </section>`;
    byId("accepted-toggle").onclick = () => { state.evidenceMode = "accepted"; renderIssue(id, filterType, filterValue); };
    byId("uncertain-toggle").onclick = () => { state.evidenceMode = "uncertain"; renderIssue(id, filterType, filterValue); };
    document.querySelectorAll("[data-month]").forEach(button => button.onclick = () => route("issue", id, ["month", button.dataset.month]));
  }

  function voiceCard(person) {
    const reach = person.network_reach;
    return `<button class="voice-card" data-route="voice" data-id="${esc(person.id)}">
      <span class="row-top"><span class="row-title">${esc(person.name)}</span><span class="meta">${fmt(person.n_episodes)} episodes</span></span>
      <span class="pill-row">
        ${person.authority != null ? `<span class="pill good">Authority ${Number(person.authority).toFixed(2)}</span>` : `<span class="pill warn">Authority unscored</span>`}
        ${reach ? `<span class="pill">Network reach ${Math.round(reach.score || 0)}</span>` : ""}
        ${person.moves_count ? `<span class="pill shift">${person.moves_count} changed view</span>` : ""}
        ${person.contrarian_count ? `<span class="pill shift">${person.contrarian_count} field disagreement</span>` : ""}
        <span class="pill">${person.direct_evidence_count} direct excerpts</span>
      </span>
      <span class="meta">${esc(person.top_topics.map(item => cap(item.topic)).join(" · "))}</span>
    </button>`;
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
    const views = [["influential", "Influential now"], ["rising", "Rising voices"], ["changed", "Changed minds"], ["contrarian", "Contrarians"], ["evidence", "Most evidence"]];
    app.innerHTML = `<section class="view"><div class="home-head"><div><div class="eyebrow">Credible voices</div><h1>Who is shaping it?</h1>
      <p class="lede">Separate direct statements from mentions, inspect expertise and evidence breadth, and follow changes and disagreements.</p></div></div>
      <input class="search" id="voice-search" aria-label="Search voices, shows, and issues" placeholder="Search people, shows, or issues" value="${esc(state.voiceQuery)}">
      <div class="briefing-tabs">${views.map(([value, label]) => `<button class="chip" data-voice-view="${value}" aria-pressed="${state.voiceView === value}">${label}</button>`).join("")}</div>
      <div class="results-status" role="status">${rows.length} voices</div>
      <div class="list">${rows.length ? rows.slice(0, 80).map(voiceCard).join("") : `<div class="empty">No voice matches this view. Clear the search or select another view.</div>`}</div>
      ${coverageFooter()}</section>`;
    byId("voice-search").oninput = event => { state.voiceQuery = event.target.value; renderVoices(); byId("voice-search")?.focus(); };
    document.querySelectorAll("[data-voice-view]").forEach(button => button.onclick = () => { state.voiceView = button.dataset.voiceView; renderVoices(); });
  }

  async function renderVoice(value) {
    navState("voices");
    const id = resolveVoice(value);
    const [voicePayload, networkPayload] = await Promise.all([load("voices"), load("network")]);
    const person = voicePayload.voices[id];
    if (!person) return renderNotFound("voices");
    const connections = (networkPayload.network.people[id] || []).slice(0, 10);
    const claims = person.top_topics.slice(0, 6);
    const direct = person.direct_evidence.slice(0, 12);
    app.innerHTML = `<section class="view">
      ${breadcrumb([{label: "Voices", route: "voices"}, {label: person.name}])}
      <div class="detail-head"><div><div class="eyebrow detail-label">Voice profile</div><h1>${esc(person.name)}</h1>
        <p class="lede">${person.authority != null ? `Authority ${Number(person.authority).toFixed(2)} · ` : "Authority unscored · "}${person.n_episodes} episodes across ${person.shows.length} shows. Only accepted direct-speech evidence appears under “What they say.”</p></div>
        <button class="coverage-badge" data-route="coverage" data-tail="voice/${esc(id)}">${person.evidence_coverage.direct} direct excerpts · ${person.evidence_coverage.shows} shows</button>
      </div>
      <div class="research-grid"><div>
        <section class="panel"><h2 class="panel-title">Current recorded views</h2><p class="panel-sub">Recurring issues in accepted direct statements—not third-party mentions.</p>
          <div class="claim-list">${claims.length ? claims.map(claim => {
            const issueId = resolveIssue(claim.topic);
            const hasIssue = INDEX.issues.some(issue => issue.id === issueId);
            return `<button class="claim-card" ${hasIssue ? `data-route="issue" data-id="${esc(issueId)}"` : "disabled aria-disabled=\"true\""}><b>${esc(cap(claim.topic))}</b><p>${claim.count} recorded positions · ${claim.positive || 0} supportive · ${claim.negative || 0} skeptical</p></button>`;
          }).join("") : `<div class="empty">No recurring accepted claim yet.</div>`}</div>
        </section>
        ${person.moves.length ? `<section class="panel"><h2 class="panel-title">Changed views</h2><div class="claim-list">${person.moves.map(move => `<button class="claim-card" data-route="issue" data-id="${esc(resolveIssue(move.topic))}"><b>${esc(cap(move.topic))}</b><p>${esc(cap(move.from))} → ${esc(cap(move.to))} · ${esc(move.from_date)} to ${esc(move.to_date)}</p></button>`).join("")}</div></section>` : ""}
        ${person.against_field.length ? `<section class="panel"><h2 class="panel-title">Where they differ from the field</h2><div class="claim-list">${person.against_field.map(row => `<button class="claim-card" data-route="issue" data-id="${esc(resolveIssue(row.topic))}"><b>${esc(cap(row.topic))}</b><p>${esc(cap(row.stance))} while ${Math.round(row.majority_share * 100)}% of recorded field positions are ${esc(row.field_majority)}</p></button>`).join("")}</div></section>` : ""}
        <section class="panel"><h2 class="panel-title">What they say</h2><p class="panel-sub">Accepted direct-speech evidence, deduplicated and ranked by quality.</p><div class="evidence-list">${direct.length ? direct.map(item => evidenceCard(item, item.issue_id || resolveIssue(item.topic))).join("") : `<div class="empty">No excerpt passes the direct-attribution publication gate yet.</div>`}</div></section>
        <section class="panel"><h2 class="panel-title">What others say about them</h2><p class="panel-sub">Third-party mentions are kept separate and never counted as this person’s position.</p><div class="evidence-list">${person.mentions.slice(0, 6).map(item => evidenceCard(item, item.issue_id || resolveIssue(item.topic))).join("") || `<div class="empty">No distinct third-party mention is attached.</div>`}</div></section>
      </div><aside>
        <section class="panel"><h2 class="panel-title">Relevant expertise</h2><p class="panel-sub">Authority and reach are different measurements.</p>
          ${person.authority != null ? `<div class="callout">Authority score ${Number(person.authority).toFixed(2)}. Interpret within the scored domain and available evidence.</div>` : `<div class="callout danger">No domain authority score is available. Do not infer expertise from frequency alone.</div>`}
          ${person.network_reach ? `<p style="margin-top:12px"><strong>Network reach:</strong> ${Math.round(person.network_reach.score || 0)}/100 across ${person.network_reach.n_links || 0} co-appearance links. Connectivity is not correctness.</p>` : ""}
        </section>
        <section class="panel"><h2 class="panel-title">Co-appearance network</h2><p class="panel-sub">People appearing in the same episodes; not endorsements or verified relationships.</p>
          <div class="claim-list">${connections.length ? connections.map(row => `<button class="claim-card" data-route="voice" data-id="${esc(row.person_id)}"><b>${esc(row.name)}</b><p>${fmt(row.weight)} shared-episode weight</p></button>`).join("") : `<div class="empty">No accepted connection.</div>`}</div>
        </section>
        <section class="panel"><h2 class="panel-title">Appears on</h2><p>${esc(person.shows.join(" · "))}</p></section>
      </aside></div>${coverageFooter()}</section>`;
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
    app.innerHTML = `<section class="view"><div class="home-head"><div><div class="eyebrow">Grounded Ask</div><h1>Ask the evidence.</h1>
      <p class="lede">Signal Desk resolves your question to tracked issues, then answers only from cited accepted evidence. Weak matches fail closed.</p></div></div>
      <form class="toolbar" id="ask-form" role="search" style="grid-template-columns:minmax(0,1fr) auto">
        <input class="search" id="ask-input" aria-label="Ask Signal Desk" value="${esc(q)}" placeholder="What changed on AI jobs? Where do credible voices disagree on agents?">
        <button class="primary" type="submit">Ask</button>
      </form>
      ${!q ? `<div class="empty">Ask about a tracked industry issue. You will get a cited brief, competing evidence, and limitations—not an uncited generated answer.</div>` :
        !best ? `<div class="callout danger">Signal Desk cannot confidently resolve that question to a tracked issue. Try a specific issue, product, company, or policy term.</div>` :
        `<section class="panel"><div class="eyebrow">Resolved to ${esc(best.name)}</div><h2 class="panel-title">Grounded answer</h2>
          ${sentence(best.brief.what_changed)}${sentence(best.brief.why_it_matters)}${best.brief.implications.map(item => sentence(item)).join("")}
          <div class="button-row"><button class="primary" data-route="issue" data-id="${esc(best.id)}">Open full decision brief</button>${coveragePill(best.brief, best.id)}</div>
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

  function stageCard(stage, index, stages) {
    const previous = stages[index - 1];
    const dims = ["shows", "episodes", "segments"];
    return `<button class="stage-card" data-stage-index="${index}" aria-pressed="false">
      <span class="stage-head"><span><span class="eyebrow">Stage ${index + 1}</span><h3>${esc(stage.label)}</h3></span><span class="explore">Inspect losses →</span></span>
      <span class="stage-metrics">${dims.map(key => `<span><b>${fmt(stage[key])}</b><small>${key}</small></span>`).join("")}</span>
      <span class="conversion-grid">${dims.map(key => {
        const prior = Number(previous?.[key] || stage[key] || 0);
        const conversion = index ? Number(stage[key] || 0) / Math.max(prior, 1) : 1;
        return `<span class="conversion">${key}: ${Math.round(conversion * 100)}% · loss ${fmt(stageLoss(stage, previous, key))}</span>`;
      }).join("")}</span>
    </button>`;
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
    const payload = await load("coverage");
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
    app.innerHTML = `<section class="view">
      <div class="home-head"><div><div class="eyebrow">Coverage &amp; Trust</div><h1>Where are the blind spots?</h1>
        <p class="lede">Understand what entered the corpus, what was lost, which sources need attention, and how coverage can bias a finding.</p></div>
        <div class="status">Live server data · ${esc(new Date(payload.generated_at).toLocaleString())}</div></div>
      <div class="callout ${missingAttempts || duplicateCount ? "danger" : ""}"><strong>Largest current risk:</strong> ${fmt(missingAttempts)} catalogued episodes have not reached transcript attempt. ${duplicateCount ? `${duplicateCount} duplicate feed group requires adjudication.` : "No duplicate feed is detected."}</div>
      <div class="coverage-grid">${funnel.stages.map(stageCard).join("")}</div>
      <div class="research-grid coverage-layout">
        <section class="panel coverage-roster"><div class="section-head" style="margin-top:0"><div><div class="eyebrow">Canonical sources</div><h2>Needs attention</h2></div><p>${funnel.canonical_show_count} canonical · ${funnel.raw_show_count} raw records</p></div>
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
          <p class="panel-sub">The public page creates a private request. Feed resolution, duplicate detection, and a bounded dry run remain mandatory gates.</p>
          <form class="enroll-form" id="enroll-form" novalidate>
            <div class="form-field"><label for="enroll-name">Show name</label><input class="search" id="enroll-name" required autocomplete="off" placeholder="Podcast name"><div class="form-error" id="name-error"></div></div>
            <div class="form-field"><label for="enroll-rss">RSS feed URL</label><input class="search" id="enroll-rss" required type="url" inputmode="url" placeholder="https://example.com/feed.xml"><div class="form-error" id="rss-error"></div></div>
            <div class="form-field"><label for="enroll-category">Category</label><select class="select" id="enroll-category"><option value="frontier_ai">Frontier AI</option><option value="enterprise_ai">Enterprise AI</option><option value="developer_tools">Developer tools</option><option value="policy_governance">Policy &amp; governance</option><option value="business_markets">Business &amp; markets</option><option value="general_technology">General technology</option></select></div>
            <div class="form-actions"><button class="primary" type="submit">Open private request</button><button class="secondary" id="copy-enroll" type="button">Copy enrollment request</button></div>
            <div class="results-status" id="enroll-status" role="status" aria-live="polite"></div>
          </form>
        </section>
        <section class="panel"><h2 class="panel-title">Metric reconciliation</h2><p class="panel-sub">Bars no longer use episode conversion to imply show or segment conversion. Every dimension is shown separately.</p>
          <p><strong>${fmt(funnel.quarantined_transcript_episodes)}</strong> quarantined transcript attempts remain outside intelligence-ready evidence.</p>
          ${funnel.duplicate_source_groups?.length ? `<div class="callout danger" style="margin-top:12px">${funnel.duplicate_source_groups.map(group => `${esc(group.names.join(" / "))} share one feed`).join("<br>")}</div>` : ""}
        </section></aside>
      </div>${coverageFooter()}</section>`;
    byId("coverage-gap").value = state.coverageGap;
    byId("coverage-search").oninput = event => { state.coverageQuery = event.target.value; renderCoverage(filterType, filterValue); byId("coverage-search")?.focus(); };
    byId("coverage-gap").onchange = event => { state.coverageGap = event.target.value; renderCoverage(filterType, filterValue); };
    document.querySelectorAll("[data-stage-index]").forEach(button => button.onclick = () => {
      state.coverageGap = Number(button.dataset.stageIndex) < 2 ? "needs_attention" : "quarantine";
      renderCoverage();
      setTimeout(() => document.querySelector(".coverage-roster")?.scrollIntoView({behavior: "smooth"}), 0);
    });
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
      byId("enroll-status").textContent = "Private enrollment request opened.";
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

  async function renderEvidence(id, filterType = "", filterValue = "") {
    navState("");
    const hit = await findEvidence(id);
    if (!hit) return renderNotFound(filterType || "briefing");
    const {issueId, issue, evidence} = hit;
    const originRoute = filterType === "voice" ? "voice" : "issue";
    const originId = filterValue || issueId;
    const url = safeUrl(evidence.source_url);
    app.innerHTML = `<section class="view evidence-page">
      ${breadcrumb([{label: originRoute === "voice" ? "Voice" : "Issue", route: originRoute, id: originId}, {label: "Evidence context"}])}
      <div class="eyebrow detail-label">${esc(cap(evidence.publishability))} evidence · ${esc(cap(evidence.attribution_type))}</div>
      <h1>${esc(issue?.name || evidence.topic || "Evidence")}</h1>
      <p class="lede">${esc(evidence.show)} · ${esc(evidence.episode)} · ${esc(evidence.date)}</p>
      <div class="context-quote">${evidence.context_before ? `<span class="context-dim">…${esc(evidence.context_before)} </span>` : ""}<mark>${esc(evidence.evidence)}</mark>${evidence.context_after ? `<span class="context-dim"> ${esc(evidence.context_after)}…</span>` : ""}</div>
      <section class="panel"><h2 class="panel-title">Evidence ledger</h2>
        <div class="metric-grid"><div class="metric"><b>${Math.round((evidence.quality_score || 0) * 100)}</b><span>quality score</span></div><div class="metric"><b>${Math.round((evidence.attribution_confidence || 0) * 100)}%</b><span>attribution confidence</span></div><div class="metric"><b>${esc(cap(evidence.publishability))}</b><span>publication state</span></div><div class="metric"><b>${esc(cap(evidence.group || "neutral"))}</b><span>recorded stance</span></div></div>
        ${evidence.quality_reasons?.length ? `<div class="callout danger" style="margin-top:14px">Reasons: ${esc(evidence.quality_reasons.join(" · "))}</div>` : `<div class="callout" style="margin-top:14px">No quality exception was recorded for this excerpt.</div>`}
        <div class="button-row"><button class="secondary" data-route="issue" data-id="${esc(issueId)}">Open issue brief</button>${url ? `<a class="primary" href="${esc(url)}" target="_blank" rel="noopener">Open original source</a>` : ""}</div>
      </section>${coverageFooter()}</section>`;
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
      app.focus({preventScroll: true});
      if (!history.state?.preserveScroll) window.scrollTo(0, 0);
    } catch (error) {
      console.error(error);
      renderError(error);
    }
  }

  document.addEventListener("click", event => {
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
    renderCoverage(parseRoute().filterType, parseRoute().filterValue);
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
