from __future__ import annotations

import json
import os
import datetime as dt
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


STATE_DIR = Path(os.environ.get("RAILWAY_UI_STATE_DIR", "ui_state")).resolve()
SNAPSHOT_PATH = STATE_DIR / "factory-state.json"
EXPORT_SNAPSHOT_PATH = Path(os.environ.get("RAILWAY_UI_EXPORT_SNAPSHOT_PATH", "exports/observer-snapshot.json")).resolve()
INGEST_TOKEN = os.environ.get("RAILWAY_UI_INGEST_TOKEN", "")
MAX_SNAPSHOT_AGE_SECONDS = int(os.environ.get("RAILWAY_UI_MAX_SNAPSHOT_AGE_SECONDS", "7200"))


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Research Intelligence Factory</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: #0e1214;
      --panel: #151c21;
      --panel-2: #10161a;
      --line: #2b3941;
      --text: #edf2f4;
      --muted: #9fb2bd;
      --blue: #5fb3ff;
      --green: #58c48f;
      --amber: #f3b44e;
      --red: #ff706f;
      --violet: #b89cff;
      --cyan: #64d4d9;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
    }
    * { box-sizing: border-box; }
    html { -webkit-text-size-adjust: 100%; }
    body { margin: 0; background: var(--bg); color: var(--text); overflow-x: hidden; }
    header { padding: 20px 28px 14px; border-bottom: 1px solid var(--line); background: #12191d; position: sticky; top: 0; z-index: 3; }
    h1 { margin: 0 0 4px; font-size: 22px; letter-spacing: 0; }
    h2 { margin: 0; font-size: 16px; letter-spacing: 0; }
    h3 { margin: 0 0 10px; font-size: 13px; color: var(--muted); font-weight: 650; letter-spacing: 0; }
    #generated { overflow-wrap: anywhere; line-height: 1.35; }
    main { padding: 18px 28px 42px; display: grid; gap: 16px; min-width: 0; }
    section { border: 1px solid var(--line); border-radius: 6px; background: var(--panel); padding: 16px; min-width: 0; }
    .section-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 12px; }
    .muted { color: var(--muted); }
    .tabs { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
    .tab { border: 1px solid var(--line); border-radius: 6px; background: var(--panel-2); color: var(--text); padding: 8px 11px; cursor: pointer; font: inherit; }
    .tab[aria-selected="true"] { border-color: var(--blue); background: #172334; }
    .view { display: none; gap: 16px; }
    .view.active { display: grid; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; }
    .metric { border: 1px solid var(--line); border-radius: 6px; padding: 12px; background: var(--panel-2); min-height: 66px; }
    .metric span { color: var(--muted); font-size: 12px; display: block; line-height: 1.25; }
    .metric b { display: block; font-size: 24px; margin-top: 5px; overflow-wrap: anywhere; }
    .split { display: grid; grid-template-columns: minmax(0, 1.3fr) minmax(300px, .9fr); gap: 14px; }
    .triple { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
    .chart-box { border: 1px solid var(--line); border-radius: 6px; background: var(--panel-2); padding: 12px; min-width: 0; }
    .chart-box svg { width: 100%; height: auto; display: block; overflow: visible; }
    .bar-row { display: grid; grid-template-columns: minmax(130px, 1fr) minmax(110px, 2fr) 56px; align-items: center; gap: 10px; margin: 7px 0; font-size: 13px; }
    .bar-track { height: 10px; background: #233039; border-radius: 999px; overflow: hidden; }
    .bar-fill { height: 100%; background: var(--blue); border-radius: 999px; }
    .heatmap { overflow-x: auto; padding-bottom: 4px; }
    .heat-row { display: grid; grid-template-columns: minmax(180px, 260px) repeat(var(--cols), 32px) 52px; gap: 4px; align-items: center; margin-bottom: 5px; font-size: 12px; }
    .bar-row > div:first-child, .heat-row > div:first-child, .spark-row > div:first-child { min-width: 0; overflow-wrap: anywhere; }
    .heat-cell { width: 32px; height: 24px; border-radius: 4px; background: #1d2830; border: 1px solid #24343e; }
    .heat-head { color: var(--muted); text-align: center; font-size: 11px; }
    .timeline-grid { display: grid; gap: 9px; }
    .spark-row { display: grid; grid-template-columns: minmax(150px, 240px) minmax(260px, 1fr) 58px; align-items: center; gap: 10px; font-size: 12px; }
    .spark-cells { display: grid; grid-auto-flow: column; grid-auto-columns: minmax(10px, 1fr); gap: 3px; align-items: end; min-height: 34px; }
    .spark-cell { border-radius: 3px 3px 0 0; min-height: 3px; background: var(--blue); opacity: .28; }
    .spark-cell.hot { opacity: .95; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; table-layout: fixed; }
    th, td { text-align: left; border-bottom: 1px solid var(--line); padding: 8px; vertical-align: top; overflow-wrap: anywhere; word-break: break-word; }
    th { color: var(--muted); font-weight: 650; }
    .flag { border-left: 4px solid var(--amber); padding: 8px 10px; background: #241d12; margin: 6px 0; border-radius: 4px; }
    .flag.critical { border-left-color: var(--red); background: #2a1717; }
    .flag.info { border-left-color: var(--blue); background: #142033; }
    .pill { display: inline-flex; align-items: center; gap: 6px; border: 1px solid var(--line); background: #172027; color: var(--muted); border-radius: 999px; padding: 3px 8px; font-size: 12px; }
    .delta-up { color: var(--green); }
    .delta-down { color: var(--red); }
    .toolbar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .icon-button { border: 1px solid var(--line); background: var(--panel-2); color: var(--text); border-radius: 6px; width: 34px; height: 34px; cursor: pointer; }
    [data-tip] { position: relative; }
    [data-tip]:hover::after, [data-tip]:focus-visible::after {
      content: attr(data-tip);
      position: absolute;
      z-index: 5;
      right: 0;
      top: calc(100% + 8px);
      width: min(320px, 80vw);
      padding: 9px 10px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #0b0f12;
      color: var(--text);
      box-shadow: 0 8px 28px rgba(0,0,0,.36);
      font-size: 12px;
      line-height: 1.35;
    }
    details { border: 1px solid var(--line); border-radius: 6px; background: var(--panel-2); padding: 10px 12px; }
    details summary { cursor: pointer; color: var(--muted); font-weight: 650; }
    @media (max-width: 900px) {
      header, main { padding-left: 14px; padding-right: 14px; }
      .split, .triple { grid-template-columns: 1fr; }
      .heat-row { grid-template-columns: minmax(150px, 210px) repeat(var(--cols), 28px) 44px; }
      .heat-cell { width: 28px; }
      .spark-row { grid-template-columns: 1fr; }
    }
    @media (max-width: 700px) {
      header { position: static; padding: 14px 10px 10px; }
      h1 { font-size: 18px; }
      h2 { font-size: 15px; line-height: 1.25; }
      main { padding: 12px 10px 32px; gap: 12px; }
      section { padding: 12px; }
      .section-head { align-items: flex-start; }
      .tabs { flex-wrap: nowrap; overflow-x: auto; padding-bottom: 4px; scrollbar-width: thin; }
      .tab { flex: 0 0 auto; min-height: 36px; white-space: nowrap; }
      .grid { grid-template-columns: repeat(auto-fit, minmax(136px, 1fr)); }
      .metric { min-height: 62px; padding: 10px; }
      .metric b { font-size: 20px; }
      .bar-row { grid-template-columns: 1fr; gap: 6px; align-items: stretch; margin: 10px 0; }
      .bar-row > div:last-child { justify-self: end; }
      .bar-track { width: 100%; }
      .chart-box { padding: 10px; }
      .heatmap { margin-left: -2px; margin-right: -2px; }
      .spark-cells { overflow-x: auto; padding-bottom: 2px; }
      table { display: block; }
      table thead { display: none; }
      table tbody, table tr, table td { display: block; width: 100%; }
      table tr { border: 1px solid var(--line); border-radius: 6px; background: var(--panel-2); margin: 8px 0; padding: 6px 0; }
      table td { border-bottom: 0; padding: 6px 8px; }
      table td::before { content: attr(data-label); display: block; color: var(--muted); font-size: 11px; line-height: 1.25; margin-bottom: 2px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Research Intelligence Factory</h1>
    <div class="muted" id="generated">Loading snapshot...</div>
    <nav class="tabs" aria-label="Dashboard views">
      <button class="tab" data-view="overview" aria-selected="true">Overview</button>
      <button class="tab" data-view="trends" aria-selected="false">Trends</button>
      <button class="tab" data-view="sources" aria-selected="false">Sources</button>
      <button class="tab" data-view="operations" aria-selected="false">Operations</button>
      <button class="tab" data-view="raw" aria-selected="false">Raw Tables</button>
    </nav>
  </header>
  <main>
    <div id="overview" class="view active">
      <section><div class="section-head"><h2>Trend Cockpit</h2><span class="pill" id="trendWindow"></span></div><div class="grid" id="heroMetrics"></div></section>
      <section><div class="section-head"><h2>Podcast Episode Inventory</h2><button class="icon-button" data-tip="Feed/catalog coverage by episode publish date. These counts include metadata-only backfilled episodes and are separate from extraction or labeling progress.">?</button></div><div class="grid" id="episodeInventoryMetrics"></div></section>
      <section><div class="section-head"><h2>What Changed By Publish Date</h2><button class="icon-button" data-tip="Concept momentum compares the latest 90 published days against the previous 90 published days using sanitized concept/event counts.">?</button></div><div class="split"><div class="chart-box"><h3>Concept Momentum</h3><div id="momentumBars"></div></div><div class="chart-box"><h3>Event Type Mix</h3><div id="eventMixBars"></div></div></div></section>
      <section><div class="section-head"><h2>Published-Time Timeline</h2><button class="icon-button" data-tip="These charts are bucketed by podcast episode published month, not by when the factory parsed or labeled the transcript.">?</button></div><div class="split"><div class="chart-box"><h3>Publication Coverage</h3><div id="publishedCoverage"></div></div><div class="chart-box"><h3>Event Types Over Time</h3><div id="eventTimeline"></div></div></div></section>
      <section><div class="section-head"><h2>Claim Subject Layer</h2><button class="icon-button" data-tip="Claim subjects group technology claims by the shared issue being discussed. Stance, speaker, source, and exact wording attach underneath as observations.">?</button></div><div class="grid" id="claimSubjectCoverage"></div></section>
      <section><div class="section-head"><h2>Intervention Flags</h2><button class="icon-button" data-tip="Operational warnings only. This page never renders transcript text, prompt text, output JSON, local paths, or secrets.">?</button></div><div id="flags"></div></section>
    </div>
    <div id="trends" class="view">
      <section><div class="section-head"><h2>Episode Inventory By Publish Month</h2><button class="icon-button" data-tip="All known podcast episodes by month of publication. This is the catalog/backfill view and is not limited to episodes with transcripts or labels.">?</button></div><div class="chart-box" id="episodeInventoryTimeline"></div></section>
      <section><div class="section-head"><h2>Published Label Coverage</h2><button class="icon-button" data-tip="Labels grouped by the podcast episode published month. This is coverage over the corpus timeline, not processing throughput.">?</button></div><div class="chart-box" id="labelVelocity"></div></section>
      <section><div class="section-head"><h2>Concept Timeline</h2><button class="icon-button" data-tip="Small-multiple time series for the highest-volume concepts, bucketed by published month.">?</button></div><div class="chart-box" id="conceptTimeline"></div></section>
      <section><div class="section-head"><h2>Claim Subject Timeline</h2><button class="icon-button" data-tip="Small-multiple time series for shared technology issues, bucketed by podcast published month. This is the primary surface for seeing what experts are discussing over time.">?</button></div><div class="chart-box" id="claimSubjectTimeline"></div></section>
      <section><div class="section-head"><h2>Claim Subject Overview</h2><button class="icon-button" data-tip="Ground-News-style issue pages: one subject, many proposition variants, stances, speakers, sources, and frames. Blindspot warnings flag one-source, one-speaker, or one-stance coverage.">?</button></div><table id="claimSubjectTop"></table></section>
      <section><div class="section-head"><h2>Expert Positions By Claim Subject</h2><button class="icon-button" data-tip="Positions are grouped by claim subject and speaker. Stance is an observation, not part of the canonical subject identity.">?</button></div><table id="claimSubjectStance"></table></section>
      <section><div class="section-head"><h2>Proposition Variants</h2><button class="icon-button" data-tip="Precise variants under each subject. Different horizons, markets, conditions, geography, or polarity stay separate while sharing the broader subject.">?</button></div><table id="claimSubjectVariants"></table></section>
      <section><div class="section-head"><h2>Frame Mix</h2><button class="icon-button" data-tip="How experts frame the same subject: economics, timeline, regulation, risk, productivity, competitiveness, and related frames.">?</button></div><table id="claimSubjectFrames"></table></section>
      <section><div class="section-head"><h2>Claim Subject Samples</h2><button class="icon-button" data-tip="Short sampled claim/proposition records for subject QA. These are extracted claim strings, not transcript excerpts.">?</button></div><table id="claimSubjectSamples"></table></section>
      <section><div class="section-head"><h2>Source Timeline</h2><button class="icon-button" data-tip="Which shows are producing discourse events over the published-time axis.">?</button></div><div class="chart-box" id="sourceTimeline"></div></section>
      <section><div class="section-head"><h2>Concept Heatmap</h2><button class="icon-button" data-tip="Top recent concepts by published month. Darker cells mean more discourse events for that concept in that month.">?</button></div><div class="chart-box heatmap" id="conceptHeatmap"></div></section>
      <section><div class="section-head"><h2>Processing Velocity</h2><button class="icon-button" data-tip="Operational throughput only: labels grouped by the date they were created in the factory. Do not use this for semantic podcast trends.">?</button></div><div class="chart-box" id="processingVelocity"></div></section>
    </div>
    <div id="sources" class="view">
      <section><div class="section-head"><h2>Source Episode Inventory</h2><button class="icon-button" data-tip="Shows with the largest known episode catalogs after RSS backfill. Transcript counts show extraction-readiness, not raw transcript text.">?</button></div><table id="sourceEpisodeCoverage"></table></section>
      <section><div class="section-head"><h2>Source Productivity</h2><button class="icon-button" data-tip="Sources ranked by labels, discourse events, observations, and event density. This helps identify which shows are producing usable signal.">?</button></div><table id="sourceProductivity"></table></section>
      <section><h2>Content Coverage</h2><div class="grid" id="contentCoverage"></div><table id="contentTypes"></table><table id="artifactTypes"></table></section>
      <section><h2>Source Yield</h2><table id="sourceYield"></table></section>
    </div>
    <div id="operations" class="view">
      <section><h2>Research Queue</h2><div class="grid" id="researchQueue"></div><table id="queueRoles"></table><table id="workerRuns"></table></section>
      <section><h2>Transcript Acquisition</h2><div class="grid" id="acquisition"></div><table id="acquisitionAttempts"></table><table id="transcriptionRuns"></table></section>
      <section><h2>Queue By Type</h2><table id="queue"></table></section>
      <section><h2>Active And Failed Jobs</h2><table id="jobs"></table></section>
    </div>
    <div id="raw" class="view">
      <section><h2>Corpus Counts</h2><div class="grid" id="counts"></div></section>
      <section><h2>Dense Coding Metrics</h2><div class="grid" id="coding"></div></section>
      <section><h2>Useful Signals</h2><div class="grid" id="signals"></div><table id="signalTypes"></table><table id="burstTerms"></table></section>
      <section><h2>Transcript Preparation</h2><table id="preparation"></table></section>
      <section><h2>Derived Tables</h2><div class="grid" id="derived"></div></section>
      <section><h2>Recent Label Runs</h2><table id="runs"></table></section>
      <section><h2>Artifacts</h2><table id="artifacts"></table></section>
    </div>
  </main>
  <script>
    const esc = (value) => String(value ?? "").replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
    const num = (value) => Number(value || 0);
    const cell = (value, label = "") => `<td data-label="${esc(label)}">${esc(value)}</td>`;
    const table = (el, headers, rows) => {
      el.innerHTML = `<thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join("")}</tr></thead><tbody>` +
        rows.map(r => `<tr>${r.map((value, index) => cell(value, headers[index] || "")).join("")}</tr>`).join("") +
        `</tbody>`;
    };
    const metricCards = (items) => items.map(([k,v,tip]) =>
      `<div class="metric"${tip ? ` data-tip="${esc(tip)}" tabindex="0"` : ""}><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join("");
    const bars = (rows, labelKey, valueKey, color = 'var(--blue)', deltaKey = null) => {
      const max = Math.max(1, ...rows.map(x => Math.abs(num(x[valueKey]))));
      return rows.map(x => {
        const value = num(x[valueKey]);
        const width = Math.max(2, Math.round(Math.abs(value) / max * 100));
        const delta = deltaKey ? num(x[deltaKey]) : null;
        const klass = delta == null ? '' : delta >= 0 ? 'delta-up' : 'delta-down';
        return `<div class="bar-row" data-tip="${esc(x.tooltip || '')}" tabindex="0">
          <div>${esc(x[labelKey])}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${width}%;background:${color}"></div></div>
          <div class="${klass}">${esc(value)}</div>
        </div>`;
      }).join("") || '<div class="muted">No data yet.</div>';
    };
    const stackedBars = (rows, categoryKey = 'label_pack') => {
      const packs = [...new Set(rows.map(x => x[categoryKey]))];
      const days = [...new Set(rows.map(x => x.day))];
      const colors = ['var(--blue)', 'var(--green)', 'var(--amber)', 'var(--violet)', 'var(--cyan)'];
      const dayTotals = Object.fromEntries(days.map(day => [day, rows.filter(x => x.day === day).reduce((a,x) => a + num(x.count), 0)]));
      const max = Math.max(1, ...Object.values(dayTotals));
      const legend = packs.map((p,i) => `<span class="pill"><span style="width:10px;height:10px;border-radius:2px;background:${colors[i % colors.length]};display:inline-block"></span>${esc(p)}</span>`).join(" ");
      const bars = days.map(day => {
        const segments = packs.map((pack, i) => {
          const count = rows.filter(x => x.day === day && x[categoryKey] === pack).reduce((a,x) => a + num(x.count), 0);
          const pct = dayTotals[day] ? count / dayTotals[day] * 100 : 0;
          return `<span data-tip="${esc(pack)}: ${count}" style="width:${pct}%;background:${colors[i % colors.length]};display:block;height:100%"></span>`;
        }).join("");
        return `<div class="bar-row"><div>${esc(day)}</div><div class="bar-track" style="display:flex;width:${Math.max(2, dayTotals[day] / max * 100)}%">${segments}</div><div>${dayTotals[day]}</div></div>`;
      }).join("");
      return `${legend}<div style="margin-top:12px">${bars}</div>`;
    };
    const timeline = (rows, seriesKey, color = 'var(--blue)') => {
      const days = [...new Set(rows.map(x => x.day))];
      const series = [...new Set(rows.map(x => x[seriesKey]))];
      const max = Math.max(1, ...rows.map(x => num(x.count)));
      return `<div class="timeline-grid">${series.map(name => {
        const values = days.map(day => rows.filter(x => x.day === day && x[seriesKey] === name).reduce((a,x) => a + num(x.count), 0));
        const total = values.reduce((a,v) => a + v, 0);
        const cells = values.map((value, index) => {
          const height = Math.max(3, Math.round(value / max * 34));
          return `<span class="spark-cell ${value ? 'hot' : ''}" data-tip="${esc(name)} in ${esc(days[index])}: ${value}" style="height:${height}px;background:${color}"></span>`;
        }).join("");
        return `<div class="spark-row" tabindex="0"><div title="${esc(name)}">${esc(name)}</div><div class="spark-cells">${cells}</div><div>${total}</div></div>`;
      }).join("") || '<div class="muted">No time series data yet.</div>'}</div>`;
    };
    const coverageTimeline = (rows) => {
      return rows.map(row => `<div class="bar-row" data-tip="${esc(row.day)}: ${esc(row.discourse_events)} events, ${esc(row.labels)} labels" tabindex="0">
        <div>${esc(row.day)}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${Math.max(2, Math.min(100, num(row.discourse_events) / Math.max(1, ...rows.map(x => num(x.discourse_events))) * 100))}%;background:var(--cyan)"></div></div>
        <div>${esc(row.discourse_events)}</div>
      </div>`).join("") || '<div class="muted">No published coverage yet.</div>';
    };
    const episodeInventoryTimeline = (rows) => {
      const max = Math.max(1, ...rows.map(x => num(x.episodes)));
      return rows.map(row => {
        const width = Math.max(2, Math.round(num(row.episodes) / max * 100));
        const tip = `${row.day}: ${row.episodes} episodes across ${row.sources} sources; ${row.episodes_with_transcripts} have transcripts`;
        return `<div class="bar-row" data-tip="${esc(tip)}" tabindex="0">
          <div>${esc(row.day)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${width}%;background:var(--amber)"></div></div>
          <div>${esc(row.episodes)}</div>
        </div>`;
      }).join("") || '<div class="muted">No episode inventory yet.</div>';
    };
    const heatmap = (payload) => {
      const days = payload.days || [];
      const concepts = payload.concepts || [];
      const max = Math.max(1, ...concepts.flatMap(c => c.values || []));
      const heads = days.map(d => `<div class="heat-head">${esc(d.slice(5))}</div>`).join("");
      const rows = concepts.map(c => {
        const cells = (c.values || []).map((v, i) => {
          const ratio = num(v) / max;
          const alpha = Math.max(.12, ratio);
          return `<div class="heat-cell" data-tip="${esc(c.concept)} on ${esc(days[i])}: ${esc(v)} events" style="background:rgba(95,179,255,${alpha})" tabindex="0"></div>`;
        }).join("");
        return `<div class="heat-row" style="--cols:${days.length}"><div title="${esc(c.concept)}">${esc(c.concept)}</div>${cells}<div>${esc(c.total)}</div></div>`;
      }).join("");
      return `<div class="heat-row" style="--cols:${days.length}"><div></div>${heads}<div class="heat-head">Total</div></div>${rows || '<div class="muted">No recent concepts yet.</div>'}`;
    };
    document.querySelectorAll('.tab').forEach(button => {
      button.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(tab => tab.setAttribute('aria-selected', String(tab === button)));
        document.querySelectorAll('.view').forEach(view => view.classList.toggle('active', view.id === button.dataset.view));
      });
    });
    fetch('/state.json', {cache: 'no-store'}).then(r => r.json()).then(data => {
      const health = data.snapshot_health || {};
      const trend = data.trend_metrics || {};
      const coding = data.coding_metrics || {};
      const signals = data.useful_signal_metrics || {};
      const researchQueue = data.research_queue_metrics || {};
      const claimSubjectCoverage = trend.claim_subject_coverage || {};
      const episodeInventory = trend.episode_inventory || {};
      const ageText = health.snapshot_age_seconds != null ? ` · age ${health.snapshot_age_seconds}s` : '';
      document.getElementById('generated').textContent = `${data.generated_at || 'No snapshot'} · ${data.privacy || ''}${ageText}`;
      document.getElementById('trendWindow').textContent = trend.trend_windows?.anchor_day ? `published label axis · through ${trend.trend_windows.anchor_day}` : `episode inventory · through ${trend.trend_windows?.episode_inventory_anchor_day || 'unknown'}`;
      const totalLabels = (coding.labels_by_pack || []).reduce((a,x) => a + num(x.count), 0);
      const latestPublishedMonth = trend.trend_windows?.anchor_day ? trend.trend_windows.anchor_day.slice(0, 7) : null;
      const latestInventoryMonth = trend.trend_windows?.episode_inventory_anchor_day ? trend.trend_windows.episode_inventory_anchor_day.slice(0, 7) : null;
      const currentLabelCount = (trend.label_velocity || []).filter(x => x.day === latestPublishedMonth).reduce((a,x) => a + num(x.count), 0);
      const currentEpisodeCount = (trend.episode_month_timeline || []).filter(x => x.day === latestInventoryMonth).reduce((a,x) => a + num(x.episodes), 0);
      const topMomentum = (trend.concept_momentum || [])[0] || {};
      document.getElementById('heroMetrics').innerHTML = metricCards([
        ['podcast episodes', episodeInventory.episodes || 0, 'All known episodes in the local catalog, including metadata-only backfilled episodes.'],
        ['latest month episodes', currentEpisodeCount, 'Episodes published in the latest represented month.'],
        ['completed labels', totalLabels, 'All labels across packs currently represented in the snapshot.'],
        ['latest published month labels', currentLabelCount, 'Labels whose podcast episode was published in the latest represented month.'],
        ['discourse events', coding.v3_1_discourse_events || coding.v3_discourse_events || 0, 'Extracted discourse events, not transcript text.'],
        ['top moving concept', topMomentum.concept || 'none', 'Largest absolute change over published-time 90-day windows.'],
        ['remote compute', researchQueue.mcp_remote_worker_enabled ? 'enabled' : 'disabled', 'Railway should remain observer and broker only.']
      ]);
      document.getElementById('episodeInventoryMetrics').innerHTML = metricCards([
        ['sources', episodeInventory.sources || 0, 'Podcast feeds represented in the local episode catalog.'],
        ['episodes', episodeInventory.episodes || 0, 'All known episode metadata rows.'],
        ['pre-2025 episodes', episodeInventory.pre_2025_episodes || 0, 'Historical episodes added by the feed backfill.'],
        ['with transcripts', episodeInventory.episodes_with_transcripts || 0, 'Episodes that currently have local transcript records.'],
        ['RSS transcript links', episodeInventory.feed_transcript_episodes || 0, 'Episodes whose feed metadata includes a creator-provided transcript link.'],
        ['transcript coverage', episodeInventory.transcript_coverage_ratio || 0, 'Episodes with transcript records divided by all known episodes.'],
        ['oldest published', episodeInventory.first_published_at || 'unknown', 'Oldest known publish date in the catalog.'],
        ['latest published', episodeInventory.last_published_at || 'unknown', 'Latest known publish date in the catalog.']
      ]);
      document.getElementById('momentumBars').innerHTML = bars(
        (trend.concept_momentum || []).map(x => ({...x, tooltip: `${x.concept}: current ${x.current_count}, previous ${x.previous_count}, change ${x.delta}`})),
        'concept', 'delta', 'var(--green)', 'delta'
      );
      document.getElementById('eventMixBars').innerHTML = bars(trend.event_type_mix || [], 'event_type', 'count', 'var(--violet)');
      document.getElementById('publishedCoverage').innerHTML = coverageTimeline(trend.publication_coverage || []);
      document.getElementById('eventTimeline').innerHTML = timeline(trend.event_type_timeline || [], 'event_type', 'var(--violet)');
      document.getElementById('claimSubjectCoverage').innerHTML = metricCards([
        ['claim subjects', claimSubjectCoverage.claim_subjects || 0, 'Shared technology issues/questions available for normalized expert-position tracking.'],
        ['proposition variants', claimSubjectCoverage.proposition_variants || 0, 'Precise claim variants under broader subjects.'],
        ['claim observations', claimSubjectCoverage.position_observations || 0, 'Speaker/source claim positions attached to subjects and variants.'],
        ['event observations', claimSubjectCoverage.event_observations || 0, 'Useful frame, term, and uncertainty events attached to subjects without becoming fake claims.'],
        ['expert positions', claimSubjectCoverage.expert_positions || 0, 'Subject-level speaker/canonical-person stance rollups.'],
        ['canonical experts', claimSubjectCoverage.expert_positions_with_canonical_person || 0, 'Expert positions grouped by resolved canonical person.'],
        ['speaker fallback', claimSubjectCoverage.expert_positions_with_speaker_fallback || 0, 'Expert positions using sanitized unresolved speaker surface.'],
        ['assigned claims', claimSubjectCoverage.assigned_claims || 0, 'Raw claims with first-class claim-subject membership.'],
        ['assigned coverage', claimSubjectCoverage.assigned_claim_coverage_ratio || 0, 'Share of raw claims covered by the claim-subject layer.'],
        ['multi-claim subjects', claimSubjectCoverage.multi_claim_subjects || 0, 'Subjects that group more than one raw claim.'],
        ['largest subject', claimSubjectCoverage.largest_subject_size || 0, 'Largest number of raw claims under one claim subject.'],
        ['subject / claim ratio', claimSubjectCoverage.subject_to_claim_ratio || 0, 'Lower is more consolidation; near 1.0 means subjects remain too specific.'],
        ['latest subjects', claimSubjectCoverage.latest_subjects_considered || 0, 'Subjects considered by the latest v2 subject run.'],
        ['latest variants', claimSubjectCoverage.latest_variants_considered || 0, 'Proposition variants considered by the latest v2 subject run.'],
        ['latest events framed', claimSubjectCoverage.latest_event_observations_framed || 0, 'Useful event-only rows attached to claim subjects by the latest run.'],
        ['quarantined templates', claimSubjectCoverage.latest_template_claims_quarantined || 0, 'Generic segment-summary claims skipped by the latest subject run.']
      ]);
      document.getElementById('episodeInventoryTimeline').innerHTML = episodeInventoryTimeline(trend.episode_month_timeline || []);
      document.getElementById('labelVelocity').innerHTML = stackedBars(trend.label_velocity || []);
      document.getElementById('conceptTimeline').innerHTML = timeline(trend.concept_timeline || [], 'concept', 'var(--green)');
      document.getElementById('claimSubjectTimeline').innerHTML = timeline(trend.claim_subject_timeline || [], 'claim_subject', 'var(--blue)');
      document.getElementById('sourceTimeline').innerHTML = timeline(trend.source_timeline || [], 'source_name', 'var(--amber)');
      document.getElementById('processingVelocity').innerHTML = stackedBars(trend.processing_velocity || []);
      document.getElementById('conceptHeatmap').innerHTML = heatmap(trend.concept_heatmap || {});
      table(document.getElementById('claimSubjectTop'), ['Claim Subject', 'Domain', 'Observations', 'Claims', 'Events', 'Variants', 'Sources', 'Speakers', 'Stances', 'First Published', 'Last Published', 'Blindspot'],
        (trend.claim_subject_top || []).map(x => [x.claim_subject, x.domain, x.observation_count, x.claim_observation_count, x.event_observation_count, x.variant_count, x.source_count, x.speaker_count, x.stance_count, x.first_published_at, x.last_published_at, x.blindspot_warning]));
      table(document.getElementById('claimSubjectStance'), ['Claim Subject', 'Speaker', 'Resolution', 'Stance', 'Observations', 'Claims', 'Events', 'Sources', 'First Published', 'Last Published'],
        (trend.claim_subject_stance || []).map(x => [x.claim_subject, x.speaker, x.speaker_resolution, x.stance, x.count, x.claim_observation_count, x.event_observation_count, x.source_count, x.first_published_at, x.last_published_at]));
      table(document.getElementById('claimSubjectVariants'), ['Claim Subject', 'Variant', 'Horizon', 'Market', 'Geography', 'Polarity', 'Observations', 'Stances'],
        (trend.claim_subject_variants || []).map(x => [x.claim_subject, x.variant_text, x.horizon, x.market_context, x.geography, x.polarity, x.observation_count, x.stance_count]));
      table(document.getElementById('claimSubjectFrames'), ['Claim Subject', 'Frame', 'Count'],
        (trend.claim_subject_frame_mix || []).map(x => [x.claim_subject, x.frame, x.count]));
      table(document.getElementById('claimSubjectSamples'), ['Claim Subject', 'Claim Observations', 'Event Observations', 'Variants', 'Sampled Variants'],
        (trend.claim_subject_samples || []).map(x => [x.claim_subject, x.observation_count, x.event_observation_count, x.variant_count, x.sampled_variants]));
      table(document.getElementById('sourceEpisodeCoverage'), ['Source', 'Category', 'Episodes', 'Pre-2025', 'With Transcripts', 'RSS Transcript Links', 'First Published', 'Last Published'],
        (trend.source_episode_coverage || []).map(x => [x.source_name, x.category, x.episodes, x.pre_2025_episodes, x.episodes_with_transcripts, x.feed_transcript_episodes, x.first_published_at, x.last_published_at]));
      table(document.getElementById('sourceProductivity'), ['Source', 'Labels', 'Events', 'Observations', 'Events / 1k Words'],
        (trend.source_productivity || []).map(x => [x.source_name, x.labels, x.discourse_events, x.observations, x.events_per_1000_words]));
      document.getElementById('counts').innerHTML = metricCards(Object.entries(data.counts || {}).map(([k,v]) => [k, v]));
      document.getElementById('researchQueue').innerHTML = metricCards([
        ['mode', researchQueue.project_mode || 'research_intelligence_factory'],
        ['local source of truth', researchQueue.local_source_of_truth ? 'yes' : 'unknown'],
        ['headless Codex workers', researchQueue.headless_codex_workers_enabled ? 'enabled' : 'disabled'],
        ['MCP broker', researchQueue.mcp_broker_enabled ? 'enabled' : 'disabled'],
        ['MCP remote workers', researchQueue.mcp_remote_worker_enabled ? 'enabled' : 'disabled'],
        ['remote contract', researchQueue.remote_worker_contract_ready ? 'ready-disabled' : 'not ready'],
        ['remote claimable', Object.entries(researchQueue.remote_claimable_by_privacy_tier || {}).map(([k,v]) => `${k}:${v}`).join(' · ') || '0'],
        ['remote import failures', researchQueue.remote_import_failures || 0],
        ['remote submissions', (researchQueue.remote_output_submissions || []).map(x => `${x.status}:${x.count}`).join(' · ') || '0'],
        ['active remote leases', (researchQueue.active_remote_leases || []).map(x => `${x.worker_role}/${x.privacy_tier}:${x.count}`).join(' · ') || '0']
      ]);
      table(document.getElementById('queueRoles'), ['Role', 'Content Type', 'Status', 'Count'],
        (researchQueue.depth_by_role || []).map(x => [x.worker_role, x.content_type, x.status, x.count]));
      table(document.getElementById('workerRuns'), ['Role', 'Status', 'Runs', 'Claimed', 'Completed', 'Failed'],
        (researchQueue.worker_runs || []).map(x => [x.worker_role, x.status, x.count, x.claimed_jobs, x.completed_jobs, x.failed_jobs]));
      const content = data.content_metrics || {};
      document.getElementById('contentCoverage').innerHTML = metricCards([
        ['supported types', (content.supported_content_types || []).length],
        ['sources', (content.source_types || []).reduce((a,x) => a + num(x.count), 0)],
        ['items', (content.content_types || []).reduce((a,x) => a + num(x.count), 0)],
        ['artifacts', (content.artifact_types || []).reduce((a,x) => a + num(x.count), 0)]
      ]);
      table(document.getElementById('contentTypes'), ['Content Type', 'Acquisition', 'Privacy', 'Count'],
        (content.content_types || []).map(x => [x.content_type, x.acquisition_status, x.privacy_tier, x.count]));
      table(document.getElementById('artifactTypes'), ['Artifact Type', 'Source Kind', 'Status', 'Count'],
        (content.artifact_types || []).map(x => [x.artifact_type, x.source_kind, x.status, x.count]));
      document.getElementById('coding').innerHTML = metricCards([
        ['v2 labels', coding.v2_labels || 0],
        ['v2 observations', coding.v2_observations || 0],
        ['observations / 1k words', coding.v2_observations_per_1000_segment_words || 0],
        ['v3 labels', coding.v3_labels || 0],
        ['v3 discourse events', coding.v3_discourse_events || 0],
        ['v3 events / 1k words', coding.v3_events_per_1000_words || coding.v3_events_per_1000_segment_words || 0],
        ['v3.1 labels', coding.v3_1_labels || 0],
        ['v3.1 discourse events', coding.v3_1_discourse_events || 0],
        ['v3.1 events / 1k words', coding.v3_1_events_per_1000_segment_words || 0],
        ['label packs', (coding.labels_by_pack || []).map(x => `${x.label_pack}:${x.count}`).join(' · ') || 'none']
      ]);
      document.getElementById('signals').innerHTML = metricCards([
        ['candidate concepts', Object.values(signals.candidate_concepts || {}).reduce((a,b) => a + num(b), 0)],
        ['needs adjudication', (signals.candidate_concepts || {}).needs_adjudication || 0],
        ['promoted aliases', (signals.concept_aliases || {}).active || 0],
        ['shift alerts', (signals.shift_signals_by_type || []).reduce((a,x) => a + num(x.count), 0)],
        ['actor stance changes', signals.actor_stance_changes || 0],
        ['v3 audit avg', signals.v3_avg_audit_score || 0],
        ['v3.1 audit avg', signals.v3_1_avg_audit_score || 0],
        ['v3.1 scale state', signals.v3_1_scale_state || 'unknown']
      ]);
      table(document.getElementById('signalTypes'), ['Signal Type', 'Count', 'Avg Score'],
        (signals.shift_signals_by_type || []).map(x => [x.signal_type, x.count, x.avg_score]));
      table(document.getElementById('burstTerms'), ['Burst Term', 'Count', 'Max Score'],
        (signals.burst_terms || []).map(x => [x.term, x.count, x.max_score]));
      document.getElementById('derived').innerHTML = metricCards(Object.entries(data.derived_table_counts || {}).map(([k,v]) => [k, v]));
      const acquisition = data.transcript_acquisition_metrics || {};
      document.getElementById('acquisition').innerHTML = Object.keys(acquisition.status_counts || {}).length
        ? metricCards(Object.entries(acquisition.status_counts || {}).map(([k,v]) => [k, v]))
        : '<div class="muted">No acquisition attempts recorded yet.</div>';
      table(document.getElementById('acquisitionAttempts'), ['Method', 'Status', 'Count'],
        (acquisition.attempt_counts || []).map(x => [x.method, x.status, x.count]));
      table(document.getElementById('transcriptionRuns'), ['Provider', 'Status', 'Count'],
        (acquisition.transcription_runs || []).map(x => [x.provider, x.status, x.count]));
      table(document.getElementById('preparation'), ['Artifact Type', 'Status', 'Count', 'Avg Boilerplate', 'Avg Quality'],
        (data.transcript_preparation_metrics || []).map(x => [x.artifact_type, x.status, x.count, x.avg_boilerplate_ratio, x.avg_quality_score]));
      table(document.getElementById('sourceYield'), ['Source', 'Category', 'Transcripts', 'Segments', 'Labels', 'Observations', 'Events', 'Obs / 1k Words'],
        (data.source_yield || []).map(x => [x.source_name, x.category, x.transcripts, x.segments, x.labels, x.observations, x.discourse_events, x.observations_per_1000_words]));
      const flags = [...(data.intervention_flags || [])];
      if (health.ok === false) flags.unshift({severity: 'critical', message: `Observer snapshot health: ${health.error || 'not ok'}`});
      document.getElementById('flags').innerHTML = flags.length
        ? flags.map(f => `<div class="flag ${esc(f.severity)}"><b>${esc(f.severity)}</b> ${esc(f.message)}</div>`).join("")
        : '<div class="muted">No intervention flags.</div>';
      table(document.getElementById('queue'), ['Lane', 'Type', 'Status', 'Count'],
        (data.job_type_counts || []).map(x => [x.lane, x.job_type, x.status, x.count]));
      table(document.getElementById('jobs'), ['Lane', 'Type', 'Status', 'Worker', 'Attempts', 'Lease', 'Updated', 'Error'],
        (data.active_jobs || []).map(x => [x.lane, x.job_type, x.status, x.lease_owner, x.attempts, x.leased_until, x.updated_at, x.error]));
      table(document.getElementById('runs'), ['Pack', 'Model', 'Status', 'Prompt', 'Output', 'Output Exists'],
        (data.recent_runs || []).map(x => [x.label_pack, x.model, x.status, x.prompt_artifact, x.output_artifact, x.output_exists]));
      table(document.getElementById('artifacts'), ['Name', 'Bytes'],
        (data.artifacts || []).map(x => [x.name, x.bytes]));
    }).catch(err => {
      document.getElementById('generated').textContent = `Snapshot unavailable: ${err}`;
    });
  </script>
</body>
</html>
"""

# Railway intentionally renders operational state only.  Private intelligence
# is served by research_factory.query_api on loopback and never embedded here.
HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Research Factory Operations</title>
  <style>
    :root { color-scheme: dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; --bg:#0e1214; --panel:#151c21; --line:#2b3941; --text:#edf2f4; --muted:#9fb2bd; --red:#ff706f; --amber:#f3b44e; --blue:#5fb3ff; }
    * { box-sizing: border-box; }
    body { margin:0; background:var(--bg); color:var(--text); }
    header { padding:20px 24px; border-bottom:1px solid var(--line); }
    h1 { margin:0 0 5px; font-size:22px; } h2 { margin:0 0 12px; font-size:16px; }
    main { display:grid; gap:14px; padding:18px 24px 36px; }
    section { min-width:0; padding:15px; border:1px solid var(--line); border-radius:7px; background:var(--panel); overflow:auto; }
    .muted { color:var(--muted); overflow-wrap:anywhere; }
    .metrics { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:9px; }
    .metric { padding:10px; border:1px solid var(--line); border-radius:6px; }
    .metric span { display:block; color:var(--muted); font-size:12px; overflow-wrap:anywhere; }
    .metric b { display:block; margin-top:4px; font-size:21px; overflow-wrap:anywhere; }
    table { width:100%; border-collapse:collapse; table-layout:fixed; font-size:13px; }
    th,td { padding:8px; border-bottom:1px solid var(--line); text-align:left; overflow-wrap:anywhere; }
    th { color:var(--muted); }
    .flag { margin:6px 0; padding:8px 10px; border-left:4px solid var(--amber); background:#241d12; }
    .flag.critical { border-left-color:var(--red); background:#2a1717; } .flag.info { border-left-color:var(--blue); background:#142033; }
    @media(max-width:600px){ header,main{padding-left:12px;padding-right:12px;} table{min-width:620px;} }
  </style>
</head>
<body>
  <header><h1>Research Factory Operations</h1><div id="generated" class="muted">Loading snapshot…</div></header>
  <main>
    <section><h2>Intervention flags</h2><div id="flags" class="muted">None.</div></section>
    <section><h2>Counts</h2><div id="counts" class="metrics"></div></section>
    <section><h2>Worker and service status</h2><div id="status" class="metrics"></div></section>
    <section><h2>Queue depth</h2><div id="queueStatus" class="metrics"></div><div id="queues"></div></section>
    <section><h2>Run activity</h2><div id="schedulerJobs"></div><div id="workerRuns"></div><div id="labelRuns"></div></section>
    <section><h2>Failures</h2><div id="failures"></div></section>
    <section><h2>Artifacts</h2><div id="artifacts"></div></section>
  </main>
  <script>
    const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    const cards = rows => `<div class="metrics">${rows.map(([k,v]) => `<div class="metric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join('')}</div>`;
    const table = (headers, rows) => rows.length ? `<table><thead><tr>${headers.map(x=>`<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(x=>`<td>${esc(x)}</td>`).join('')}</tr>`).join('')}</tbody></table>` : '<div class="muted">No rows.</div>';
    fetch('/state.json', {cache:'no-store'}).then(response => response.json()).then(data => {
      const health = data.snapshot_health || {};
      const age = health.snapshot_age_seconds == null ? '' : ` · age ${health.snapshot_age_seconds}s`;
      document.getElementById('generated').textContent = `${data.generated_at || 'No snapshot'} · ${data.contract_version || ''}${age}`;
      const flags = [...(data.intervention_flags || [])];
      if (health.ok === false) flags.unshift({severity:'critical',message:`Observer snapshot health: ${health.error || 'not ok'}`});
      document.getElementById('flags').innerHTML = flags.length ? flags.map(x=>`<div class="flag ${esc(x.severity)}"><b>${esc(x.severity)}</b> ${esc(x.message)}</div>`).join('') : '<div class="muted">No intervention flags.</div>';
      document.getElementById('counts').innerHTML = cards(Object.entries(data.counts || {}));
      const runs = data.runs || {}, workers = runs.worker_status || {}, services = runs.service_status || {};
      document.getElementById('status').innerHTML = cards([
        ...Object.entries(workers).map(([name,value]) => [`worker: ${name}`, `${value.state || 'unknown'} · active claims ${value.active_claims || 0}`]),
        ...Object.entries(services).map(([name,value]) => [`service: ${name}`, value.state || 'unknown'])
      ]);
      const queues = data.queues || {};
      document.getElementById('queueStatus').innerHTML = cards(Object.entries(queues.by_status || {}));
      document.getElementById('queues').innerHTML = table(['Lane','Job type','Status','Count'], (queues.by_lane_type_status || []).map(x=>[x.lane,x.job_type,x.status,x.count]));
      const scheduler = runs.scheduler || {};
      document.getElementById('schedulerJobs').innerHTML = table(['PIF scheduler job','Enabled'], (scheduler.jobs || []).map(x=>[x.name,x.enabled]));
      document.getElementById('workerRuns').innerHTML = table(['Worker role','Status','Runs','Claimed','Completed','Failed','Last observed'], (runs.worker_runs || []).map(x=>[x.worker_role,x.status,x.count,x.claimed_jobs,x.completed_jobs,x.failed_jobs,x.last_observed_at]));
      document.getElementById('labelRuns').innerHTML = table(['Label pack','Model','Status','Prompt artifact','Output artifact','Output exists'], (runs.label_runs || []).map(x=>[x.label_pack,x.model,x.status,x.prompt_artifact,x.output_artifact,x.output_exists]));
      const failures = data.failures || {};
      document.getElementById('failures').innerHTML = table(['Lane','Job type','Count'], (failures.jobs || []).map(x=>[x.lane,x.job_type,x.count]));
      document.getElementById('artifacts').innerHTML = table(['Type','Count','Bytes','Most recent'], (data.artifacts || []).map(x=>[x.artifact_type,x.count,x.bytes,x.most_recent_at]));
    }).catch(error => { document.getElementById('generated').textContent = `Snapshot unavailable: ${error}`; });
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self) -> None:
        if self.path in {"/", "/index.html"}:
            self._send(HTTPStatus.OK, "text/html; charset=utf-8", b"", include_body=False)
        elif self.path == "/state.json":
            self._send(HTTPStatus.OK, "application/json", b"", include_body=False)
        elif self.path == "/readyz":
            self._send(HTTPStatus.OK, "application/json", b"", include_body=False)
        elif self.path == "/healthz":
            status, payload = _health()
            self._send(status, "application/json", b"", include_body=False)
        elif self.path == "/favicon.ico":
            self._send(HTTPStatus.NO_CONTENT, "image/x-icon", b"", include_body=False)
        else:
            self._send(HTTPStatus.NOT_FOUND, "text/plain", b"", include_body=False)

    def do_GET(self) -> None:
        if self.path in {"/", "/index.html"}:
            self._send(HTTPStatus.OK, "text/html; charset=utf-8", HTML.encode("utf-8"))
        elif self.path == "/state.json":
            if SNAPSHOT_PATH.exists():
                self._send(HTTPStatus.OK, "application/json", _state_body())
            else:
                self._send(HTTPStatus.OK, "application/json", _state_body())
        elif self.path == "/healthz":
            status, payload = _health()
            self._send(status, "application/json", json.dumps(payload, ensure_ascii=True).encode("utf-8"))
        elif self.path == "/readyz":
            self._send(HTTPStatus.OK, "application/json", b'{"ok":true}')
        elif self.path == "/favicon.ico":
            self._send(HTTPStatus.NO_CONTENT, "image/x-icon", b"")
        else:
            self._send(HTTPStatus.NOT_FOUND, "text/plain", b"not found")

    def do_POST(self) -> None:
        if self.path != "/ingest-snapshot":
            self._send(HTTPStatus.NOT_FOUND, "text/plain", b"not found")
            return
        if not INGEST_TOKEN:
            self._send(HTTPStatus.SERVICE_UNAVAILABLE, "application/json", b'{"ok":false,"error":"ingest token not configured"}')
            return
        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {INGEST_TOKEN}":
            self._send(HTTPStatus.UNAUTHORIZED, "application/json", b'{"ok":false,"error":"unauthorized"}')
            return
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1_000_000:
            self._send(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "application/json", b'{"ok":false,"error":"snapshot too large"}')
            return
        body = self.rfile.read(length)
        try:
            parsed = json.loads(body)
            _validate_snapshot_contract(parsed, reject_unknown=True)
            parsed = _sanitize_snapshot(parsed)
        except Exception:
            self._send(HTTPStatus.BAD_REQUEST, "application/json", b'{"ok":false,"error":"invalid_snapshot_contract"}')
            return
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_PATH.write_bytes(json.dumps(parsed, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8"))
        self._send(HTTPStatus.OK, "application/json", b'{"ok":true}')

    def log_message(self, format: str, *args) -> None:
        return

    def _send(self, status: HTTPStatus, content_type: str, body: bytes, *, include_body: bool = True) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)


def main() -> int:
    if not INGEST_TOKEN and os.environ.get("RAILWAY_ENVIRONMENT"):
        raise RuntimeError("RAILWAY_UI_INGEST_TOKEN must be set in Railway")
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
    return 0


def _health() -> tuple[HTTPStatus, dict[str, object]]:
    snapshot_path = _best_snapshot_path()
    if not snapshot_path.exists():
        return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "snapshot_missing"}
    try:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        _validate_snapshot_contract(payload, reject_unknown=True)
        generated_at = payload.get("generated_at")
        if not generated_at:
            return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "snapshot_generated_at_missing"}
        parsed = dt.datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        age_seconds = (dt.datetime.now(dt.timezone.utc) - parsed.astimezone(dt.timezone.utc)).total_seconds()
        if age_seconds > MAX_SNAPSHOT_AGE_SECONDS:
            return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "snapshot_stale", "age_seconds": int(age_seconds)}
        return HTTPStatus.OK, {"ok": True, "snapshot_age_seconds": int(age_seconds)}
    except Exception:
        return HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": "snapshot_invalid"}


def _state_body() -> bytes:
    status, health = _health()
    snapshot_path = _best_snapshot_path()
    if snapshot_path.exists():
        try:
            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {
                "contract_version": "railway-operational-v2",
                "generated_at": None,
                "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
                "counts": {},
                "queues": {},
                "runs": {},
                "failures": {},
                "artifacts": [],
                "intervention_flags": [
                    {"severity": "warning", "message": "Published snapshot is invalid."}
                ],
            }
            health = {"ok": False, "error": "snapshot_invalid"}
    else:
        payload = {
            "contract_version": "railway-operational-v2",
            "generated_at": None,
            "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
            "counts": {},
            "queues": {},
            "runs": {},
            "failures": {},
            "artifacts": [],
            "intervention_flags": [{"severity": "info", "message": "No snapshot has been published yet."}],
        }
    payload["snapshot_health"] = health
    payload = _sanitize_snapshot(payload)
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8")


def _best_snapshot_path() -> Path:
    candidates = [SNAPSHOT_PATH, EXPORT_SNAPSHOT_PATH]
    existing = [path for path in candidates if path.exists()]
    if not existing:
        return SNAPSHOT_PATH
    # Choose by metadata before parsing. A stale local state file should not be
    # able to block health/state endpoints when a fresh export snapshot exists.
    return max(existing, key=lambda path: path.stat().st_mtime)


PUBLIC_SNAPSHOT_FIELDS = {
    "contract_version",
    "generated_at",
    "privacy",
    "counts",
    "queues",
    "runs",
    "failures",
    "artifacts",
    "intervention_flags",
    "snapshot_health",
}


def _validate_snapshot_contract(payload: object, *, reject_unknown: bool) -> None:
    if not isinstance(payload, dict):
        raise ValueError("snapshot must be a JSON object")
    if payload.get("contract_version") != "railway-operational-v2":
        raise ValueError("snapshot operational contract version missing")
    if payload.get("privacy") != "sanitized_operational_snapshot_no_raw_transcripts":
        raise ValueError("snapshot privacy marker missing")
    required = {
        "counts": dict,
        "queues": dict,
        "runs": dict,
        "failures": dict,
        "artifacts": list,
        "intervention_flags": list,
    }
    for key, expected_type in required.items():
        if not isinstance(payload.get(key), expected_type):
            raise ValueError(f"snapshot missing required operational field: {key}")
    if reject_unknown:
        unknown = sorted(set(payload) - PUBLIC_SNAPSHOT_FIELDS)
        if unknown:
            raise ValueError("snapshot contains non-operational fields")


def _sanitize_snapshot(payload: dict) -> dict:
    counts = {
        _safe_identifier(key): int(value)
        for key, value in (payload.get("counts") or {}).items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }
    queues = payload.get("queues") if isinstance(payload.get("queues"), dict) else {}
    runs = payload.get("runs") if isinstance(payload.get("runs"), dict) else {}
    failures = payload.get("failures") if isinstance(payload.get("failures"), dict) else {}
    sanitized = {
        "contract_version": "railway-operational-v2",
        "generated_at": _safe_timestamp(payload.get("generated_at")),
        "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
        "counts": counts,
        "queues": {
            "by_status": _numeric_map(queues.get("by_status")),
            "by_lane_type_status": _sanitize_rows(queues.get("by_lane_type_status"), {"lane", "job_type", "status", "count"}),
            "by_role_content_status": _sanitize_rows(queues.get("by_role_content_status"), {"worker_role", "content_type", "status", "count"}),
            "privacy_tiers": _numeric_map(queues.get("privacy_tiers")),
            "remote_claimable_by_privacy_tier": _numeric_map(queues.get("remote_claimable_by_privacy_tier")),
        },
        "runs": {
            "label_runs": _sanitize_rows(
                runs.get("label_runs"),
                {"label_pack", "model", "status", "prompt_artifact", "output_artifact", "output_exists", "claimed_at", "completed_at"},
            ),
            "worker_runs": _sanitize_rows(
                runs.get("worker_runs"),
                {"worker_role", "status", "count", "claimed_jobs", "completed_jobs", "failed_jobs", "last_observed_at"},
            ),
            "scheduler": _sanitize_scheduler(runs.get("scheduler")),
            "worker_status": _sanitize_status_map(runs.get("worker_status")),
            "service_status": _sanitize_status_map(runs.get("service_status")),
        },
        "failures": {
            "jobs": _sanitize_rows(failures.get("jobs"), {"lane", "job_type", "count"}),
            "expired_claims": _sanitize_rows(failures.get("expired_claims"), {"lane", "job_type", "count"}),
            "label_runs": _sanitize_rows(failures.get("label_runs"), {"label_pack", "model", "count"}),
            "output_submissions": _sanitize_rows(failures.get("output_submissions"), {"status", "count"}),
            "remote_import_failures": _safe_int(failures.get("remote_import_failures")),
            "missing_claimed_run_outputs": _safe_int(failures.get("missing_claimed_run_outputs")),
            "queue_sync": _safe_identifier(failures.get("queue_sync") or "unknown"),
        },
        "artifacts": _sanitize_rows(payload.get("artifacts"), {"artifact_type", "count", "bytes", "most_recent_at"}),
        "intervention_flags": _sanitize_flags(payload.get("intervention_flags")),
    }
    health = payload.get("snapshot_health")
    if isinstance(health, dict):
        sanitized["snapshot_health"] = {
            key: _sanitize_operational_value(value, key=key)
            for key, value in health.items()
            if key in {"ok", "error", "mode", "snapshot_age_seconds", "age_seconds"}
        }
    return sanitized


def _numeric_map(value: object) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    return {
        _safe_identifier(key): int(item)
        for key, item in value.items()
        if isinstance(item, (int, float)) and not isinstance(item, bool)
    }


def _sanitize_rows(value: object, allowed_fields: set[str]) -> list[dict]:
    if not isinstance(value, list):
        return []
    rows: list[dict] = []
    for item in value[:200]:
        if not isinstance(item, dict):
            continue
        rows.append({
            key: _sanitize_operational_value(child, key=key)
            for key, child in item.items()
            if key in allowed_fields
        })
    return rows


def _sanitize_status_map(value: object) -> dict[str, dict]:
    if not isinstance(value, dict):
        return {}
    return {
        _safe_identifier(name): {
            key: _sanitize_operational_value(child, key=key)
            for key, child in status.items()
            if key in {"state", "active_claims", "evidence"}
        }
        for name, status in value.items()
        if isinstance(status, dict)
    }


def _sanitize_scheduler(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {"state": "unavailable", "jobs": [], "enabled_count": 0, "disabled_count": 0}
    jobs = []
    for item in (value.get("jobs") or [])[:100]:
        if not isinstance(item, dict):
            continue
        name = _safe_token(item.get("name"))
        if name and name != "redacted":
            jobs.append({"name": name, "enabled": bool(item.get("enabled"))})
    return {
        "state": _safe_token(value.get("state")) or "unavailable",
        "jobs": jobs,
        "enabled_count": _safe_int(value.get("enabled_count")),
        "disabled_count": _safe_int(value.get("disabled_count")),
        "evidence": _safe_token(value.get("evidence")),
    }


def _sanitize_flags(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    allowed_messages = (
        re.compile(r"^\d+ failed job\(s\) need review\.$"),
        re.compile(r"^\d+ claimed job lease\(s\) have expired\.$"),
        re.compile(r"^\d+ claimed run output artifact\(s\) are missing\.$"),
        re.compile(r"^Queue envelope refresh was skipped because the database was locked\.$"),
        re.compile(r"^Large claimed-job backlog; check worker health\.$"),
        re.compile(r"^Segments exist but no completed labels are recorded\.$"),
        re.compile(r"^No snapshot has been published yet\.$"),
        re.compile(r"^Published snapshot is invalid\.$"),
    )
    flags: list[dict[str, str]] = []
    for item in value[:100]:
        if not isinstance(item, dict):
            continue
        severity = str(item.get("severity") or "info").lower()
        message = _sanitize_text(str(item.get("message") or ""))
        if severity not in {"info", "warning", "critical"}:
            severity = "warning"
        if message and any(pattern.fullmatch(message) for pattern in allowed_messages):
            flags.append({"severity": severity, "message": message})
    return flags


def _safe_int(value: object) -> int:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0


def _safe_identifier(value: object) -> str:
    return _safe_token(value) or "unknown"


def _safe_token(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not re.fullmatch(r"[A-Za-z0-9_.:@/+\-]{1,100}", text):
        return "redacted"
    return text


def _safe_timestamp(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?", text):
        return None
    return text[:40]


def _sanitize_operational_value(value: object, *, key: str) -> object:
    numeric_fields = {
        "count",
        "claimed_jobs",
        "completed_jobs",
        "failed_jobs",
        "active_claims",
        "bytes",
        "snapshot_age_seconds",
        "age_seconds",
    }
    timestamp_fields = {"claimed_at", "completed_at", "last_observed_at", "most_recent_at"}
    token_fields = {
        "lane",
        "job_type",
        "status",
        "worker_role",
        "content_type",
        "label_pack",
        "model",
        "prompt_artifact",
        "output_artifact",
        "artifact_type",
        "state",
        "evidence",
        "error",
        "queue_sync",
        "mode",
    }
    if key in numeric_fields:
        return _safe_int(value)
    if key in timestamp_fields:
        return _safe_timestamp(value)
    if key == "output_exists" or key == "ok":
        return bool(value)
    if key in token_fields:
        return _safe_token(value)
    return None


def _sanitize_value(value, *, key: str = ""):
    if isinstance(value, dict):
        return {str(child_key): _sanitize_value(child, key=str(child_key)) for child_key, child in value.items() if _allowed_public_key(str(child_key))}
    if isinstance(value, list):
        limit = 400 if key in {"episode_month_timeline", "episode_year_timeline"} else 200
        return [_sanitize_value(item, key=key) for item in value[:limit]]
    if isinstance(value, str):
        return _sanitize_text(value)
    return value


def _allowed_public_key(key: str) -> bool:
    blocked = {"target_id", "segment_id", "episode_id", "label_id", "job_id", "id", "prompt_path", "output_path", "source_url", "url"}
    lower = key.lower()
    if lower in blocked or lower.endswith("_url") or lower.endswith("_path"):
        return False
    private_text_keys = {"raw_transcript_text", "transcript_text", "prompt_text", "output_json", "raw_text"}
    return lower not in private_text_keys


def _sanitize_text(value: str) -> str:
    text = re.sub(r"/Users/[^\s\"']+", "<local-path>", value)
    text = re.sub(r"https?://[^\s\"')]+", "<url>", text)
    text = re.sub(r"\b(ep|seg|tr|lbl|run|aud)_[a-f0-9]{12,}\b", r"\1_<id>", text)
    return text[:500]


if __name__ == "__main__":
    raise SystemExit(main())
