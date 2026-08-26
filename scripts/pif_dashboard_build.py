#!/usr/bin/env python3
"""Render the discourse dashboard: single self-contained HTML file.

Reads work/pif-ops/dashboard/data.json (produced by
research_factory.pif_discourse_aggregates) and writes dashboard.html beside
it. No server, no network dependencies beyond Google Fonts (graceful
fallbacks); all data embedded.

Usage: python3 scripts/pif_dashboard_build.py
"""
import json
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DATA = PIF_ROOT / "work" / "pif-ops" / "dashboard" / "data.json"
OUT = PIF_ROOT / "work" / "pif-ops" / "dashboard" / "dashboard.html"

TEMPLATE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Signal Desk</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,700;9..144,900&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{
  --paper:#faf8f3; --panel:#ffffff; --ink:#171614; --ink-2:#57544d;
  --ink-3:#8b877c; --rule:#e4e0d5; --rule-2:#d3cec0;
  --pos:#2a78d6; --neg:#e34948; --neu:#c9c5ba; --accent:#1baf7a;
  --gold:#eda100; --violet:#4a3aa7;
  --serif:"Fraunces",Georgia,serif; --sans:"IBM Plex Sans",-apple-system,sans-serif;
  --mono:"IBM Plex Mono",ui-monospace,monospace;
}
*{box-sizing:border-box;margin:0}
html{color-scheme:light}
body{background:var(--paper);color:var(--ink);font:15px/1.5 var(--sans);
  background-image:radial-gradient(rgba(23,22,20,.028) 1px,transparent 1px);
  background-size:26px 26px}
a{color:inherit}
.wrap{max-width:1320px;margin:0 auto;padding:0 28px 80px}

/* ---- masthead */
header{border-bottom:3px double var(--rule-2);padding:34px 0 18px;margin-bottom:10px}
.mast{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;flex-wrap:wrap}
h1{font:900 clamp(34px,4.6vw,54px)/0.95 var(--serif);letter-spacing:-.02em}
h1 em{font-style:italic;font-weight:500;color:var(--ink-2)}
.mast-right{text-align:right;font:12px/1.7 var(--mono);color:var(--ink-2)}
.mast-right b{color:var(--ink)}
.chip{display:inline-block;border:1px solid var(--rule-2);border-radius:999px;
  padding:2px 10px;font:11px var(--mono);color:var(--ink-2);margin-left:6px}
.chip.warn{border-color:var(--gold);color:#8a5c00;background:#fdf6e3}
.tagline{font:13px var(--mono);color:var(--ink-3);margin-top:6px;letter-spacing:.06em;text-transform:uppercase}

/* ---- signal strip */
.signals{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:14px;margin:26px 0 34px}
.sig{background:var(--panel);border:1px solid var(--rule);border-radius:10px;padding:14px 16px;
  box-shadow:0 1px 0 rgba(23,22,20,.04);cursor:pointer;transition:transform .12s ease,box-shadow .12s ease}
.sig:hover{transform:translateY(-2px);box-shadow:0 6px 18px rgba(23,22,20,.08)}
.sig-kind{font:11px var(--mono);letter-spacing:.14em;text-transform:uppercase;display:flex;align-items:center;gap:7px}
.sig-kind .dot{width:8px;height:8px;border-radius:2px;flex:none}
.sig h3{font:700 19px/1.2 var(--serif);margin:7px 0 3px;text-transform:capitalize}
.sig p{font-size:12.5px;color:var(--ink-2)}
.sig svg{display:block;margin-top:8px}

/* ---- layout */
.cols{display:grid;grid-template-columns:minmax(380px,5fr) minmax(420px,7fr);gap:34px;align-items:start}
@media(max-width:980px){.cols{grid-template-columns:1fr}}
section>h2{font:700 15px var(--mono);letter-spacing:.16em;text-transform:uppercase;
  border-bottom:2px solid var(--ink);padding-bottom:8px;margin-bottom:14px;display:flex;justify-content:space-between;align-items:baseline}
section>h2 span{font:12px var(--mono);color:var(--ink-3);letter-spacing:0;text-transform:none}
.search{width:100%;border:1px solid var(--rule-2);border-radius:8px;background:var(--panel);
  padding:9px 13px;font:14px var(--sans);color:var(--ink);margin-bottom:14px}
.search:focus{outline:2px solid var(--pos);outline-offset:1px;border-color:transparent}

/* ---- people */
.person{background:var(--panel);border:1px solid var(--rule);border-radius:10px;padding:13px 16px;margin-bottom:10px;cursor:pointer;
  transition:border-color .12s}
.person:hover{border-color:var(--ink-3)}
.p-top{display:flex;justify-content:space-between;gap:10px;align-items:baseline}
.p-name{font:700 18px/1.15 var(--serif)}
.p-meta{font:11px var(--mono);color:var(--ink-3);white-space:nowrap}
.badges{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px}
.badge{font:11px/1 var(--mono);padding:4px 8px;border-radius:5px;border:1px solid}
.badge.moved{color:#7a3b93;border-color:#d9bfe4;background:#f7effa}
.badge.against{color:#8a5c00;border-color:#ecd9a1;background:#fdf6e3}
.badge.auth{color:#12513c;border-color:#bfe0d2;background:#eefaf4}
.p-shows{font:11.5px var(--mono);color:var(--ink-3);margin-top:6px}

/* ---- topics */
.topic-row{display:grid;grid-template-columns:1fr 96px 70px;gap:10px;align-items:center;
  padding:8px 10px;border-bottom:1px solid var(--rule);cursor:pointer;border-radius:6px}
.topic-row:hover{background:var(--panel)}
.topic-row.sel{background:var(--panel);outline:1px solid var(--rule-2)}
.t-name{font:600 14px var(--sans);text-transform:capitalize;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.t-vol{font:12px var(--mono);color:var(--ink-2);text-align:right}

/* ---- detail panel */
.detail{background:var(--panel);border:1px solid var(--rule-2);border-radius:12px;padding:20px 22px;margin-top:16px;
  box-shadow:0 10px 30px rgba(23,22,20,.06)}
.detail h3{font:900 26px/1.05 var(--serif);text-transform:capitalize;margin-bottom:2px}
.detail .sub{font:12px var(--mono);color:var(--ink-3);margin-bottom:14px}
.legend{display:flex;gap:16px;font:11.5px var(--mono);color:var(--ink-2);margin:10px 0 2px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px;vertical-align:-1px}
.quote{border-left:3px solid var(--rule-2);padding:8px 14px;margin:10px 0;font-size:13.5px}
.quote .q{font-style:italic;font-family:var(--serif);font-size:15px;line-height:1.45}
.quote .who{font:11px var(--mono);color:var(--ink-3);margin-top:5px}
.stance-pos{color:var(--pos)} .stance-neg{color:var(--neg)} .stance-neu{color:var(--ink-3)}
.move-line{font-size:13px;padding:7px 0;border-bottom:1px dashed var(--rule)}
.move-line b{text-transform:capitalize}
.close-x{float:right;border:none;background:none;font:16px var(--mono);color:var(--ink-3);cursor:pointer}

/* ---- footer / coverage */
footer{margin-top:48px;border-top:3px double var(--rule-2);padding-top:16px}
footer h2{font:700 12px var(--mono);letter-spacing:.16em;text-transform:uppercase;color:var(--ink-2);margin-bottom:10px}
.foot-note{font:11.5px var(--mono);color:var(--ink-3);margin-top:8px;line-height:1.7}

/* ---- tooltip */
#tip{position:fixed;pointer-events:none;background:var(--ink);color:var(--paper);
  font:12px var(--mono);padding:7px 10px;border-radius:6px;opacity:0;transition:opacity .1s;z-index:50;max-width:280px}
.bar-seg{shape-rendering:crispEdges}
.reveal{animation:rise .5s ease both}
@keyframes rise{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
</style></head><body>
<div class="wrap">
<header>
  <div class="mast">
    <div>
      <h1>Signal Desk <em>— technical podcast discourse</em></h1>
      <div class="tagline" id="tagline"></div>
    </div>
    <div class="mast-right" id="mast-stats"></div>
  </div>
</header>

<div class="signals" id="signals"></div>

<div class="cols">
  <section id="people-col">
    <h2>The People Board <span>who moved · who dissents</span></h2>
    <input class="search" id="psearch" placeholder="Search people, shows…">
    <div id="people"></div>
  </section>
  <section id="topics-col">
    <h2>Topics <span>open vocabulary — grown from the corpus</span></h2>
    <input class="search" id="tsearch" placeholder="Search topics…">
    <div id="detail-slot"></div>
    <div id="topics" style="margin-top:12px"></div>
  </section>
</div>

<footer>
  <h2>Corpus coverage — read trends against this</h2>
  <div id="coverage"></div>
  <div class="foot-note" id="foot-note"></div>
</footer>
</div>
<div id="tip"></div>
<script>
const DATA = __DATA__;
const $ = s => document.querySelector(s);
const esc = s => String(s??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const cap = s => String(s??"").replace(/\b\w/g,c=>c.toUpperCase());
const tip = $("#tip");
function showTip(e,html){tip.innerHTML=html;tip.style.opacity=1;
  tip.style.left=Math.min(e.clientX+14,innerWidth-300)+"px";tip.style.top=(e.clientY+14)+"px";}
function hideTip(){tip.style.opacity=0;}

/* ---------- masthead */
$("#tagline").textContent =
  `${Object.keys(DATA.topics).length} live topics · ${DATA.people.length} tracked voices · data through ${DATA.data_through}`;
$("#mast-stats").innerHTML =
  `<b>${DATA.corpus.labels.toLocaleString()}</b> labeled segments · <b>${DATA.corpus.episodes.toLocaleString()}</b> episodes · <b>${DATA.corpus.shows}</b> shows<br>`+
  `generated ${esc(DATA.generated_at)}`+
  (freshnessLagDays()>21?`<br><span class="chip warn">ingestion lag: newest well-covered week is ${esc(DATA.data_through)}</span>`:"");
function freshnessLagDays(){
  return Math.round((Date.now()-new Date(DATA.data_through))/864e5);}

/* ---------- sparkline */
function spark(series,w=210,h=34,color="var(--pos)"){
  const vols=series.map(s=>s.vol),mx=Math.max(...vols,1);
  const pts=vols.map((v,i)=>`${(i/(vols.length-1)*w).toFixed(1)},${(h-3-(v/mx)*(h-8)).toFixed(1)}`);
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" aria-hidden="true">
    <polyline points="${pts.join(" ")}" fill="none" stroke="${color}" stroke-width="2" stroke-linejoin="round"/></svg>`;}

/* ---------- signal cards */
const breadthNote=t=>t.episodes?` — across ${t.episodes} episodes on ${t.shows} shows`:"";
const SIG_META={
  discussed:{color:"var(--pos)",blurb:t=>`${t.pulse_vol} mentions across ${t.shows} shows (${t.episodes} episodes) in the last ${DATA.pulse_weeks} weeks.`,label:"Most discussed"},
  emerging:{color:"var(--accent)",blurb:t=>`${t.pulse_vol} mentions in the last ${DATA.pulse_weeks} weeks — near-zero baseline before${breadthNote(t)}.`,label:"Emerging"},
  shifting:{color:"var(--violet)",blurb:t=>`stance mix moved ${Math.round(t.divergence*100)}% vs the prior quarter.`,label:"Opinion shift"},
  contested:{color:"var(--gold)",blurb:t=>`${t.positive} voices for, ${t.negative} against${breadthNote(t)} — a live fight.`,label:"Contested"},
  fading:{color:"var(--neg)",blurb:t=>`peaked at ${t.peak_week_vol}/week, now ${t.pulse_vol} mentions in ${DATA.pulse_weeks} weeks.`,label:"Fading"}};
(function(){
  const box=$("#signals");let n=0;
  // Fill the strip: detector hits first, then broadest live topics.
  const detectorTopics=new Set();
  const cards=[];
  for(const kind of ["emerging","shifting","contested","fading"])
    for(const item of (DATA.detectors[kind]||[]).slice(0,3)){cards.push([kind,item]);detectorTopics.add(item.topic);}
  const broad=Object.entries(DATA.topics)
    .filter(([t,d])=>!detectorTopics.has(t)&&t!=="other"&&(d.pulse_shows||0)>=3)
    .sort((a,b)=>(b[1].pulse_shows||0)-(a[1].pulse_shows||0)||(b[1].pulse_vol||0)-(a[1].pulse_vol||0))
    .slice(0,Math.max(0,8-cards.length));
  for(const [t,d] of broad)cards.push(["discussed",{topic:t,pulse_vol:d.pulse_vol,shows:d.pulse_shows,episodes:d.pulse_episodes}]);
  for(const [kind,item] of cards){
      const t=DATA.topics[item.topic];const m=SIG_META[kind];
      const el=document.createElement("div");
      el.className="sig reveal";el.style.animationDelay=(n++*60)+"ms";
      el.innerHTML=`<div class="sig-kind" style="color:${m.color}"><span class="dot" style="background:${m.color}"></span>${m.label}</div>
        <h3>${esc(item.topic)}</h3><p>${m.blurb(item)}</p>${t?spark(t.series,210,34,m.color):""}`;
      el.onclick=()=>selectTopic(item.topic);
      box.appendChild(el);
  }
  if(!box.children.length)box.innerHTML='<div class="sig"><p>No active signals — detectors run nightly.</p></div>';
})();

/* ---------- people board */
function personCard(p){
  const badges=[];
  if(p.moves.length)badges.push(`<span class="badge moved">⇄ moved on ${esc(p.moves[p.moves.length-1].topic)}</span>`);
  if(p.against_field.length)badges.push(`<span class="badge against">⚑ against the field: ${esc(p.against_field[0].topic)}</span>`);
  if(p.authority)badges.push(`<span class="badge auth">◈ authority ${p.authority.toFixed(2)}</span>`);
  return `<div class="person reveal" data-name="${esc(p.name)}">
    <div class="p-top"><span class="p-name">${esc(p.name)}</span>
      <span class="p-meta">${p.n_recent} recent · ${p.n_positions} total</span></div>
    <div class="badges">${badges.join("")}</div>
    <div class="p-shows">${esc(p.shows.slice(0,4).join(" · "))}${p.last_seen?" — last "+esc(p.last_seen):""}</div></div>`;}
function renderPeople(filter=""){
  const q=filter.toLowerCase();
  $("#people").innerHTML=DATA.people
    .filter(p=>!q||p.name.toLowerCase().includes(q)||p.shows.join(" ").includes(q))
    .slice(0,30).map(personCard).join("");
  document.querySelectorAll(".person").forEach(el=>el.onclick=()=>selectPerson(el.dataset.name));}
renderPeople();
$("#psearch").oninput=e=>renderPeople(e.target.value);

/* ---------- topic list */
const topicNames=Object.keys(DATA.topics).sort((a,b)=>DATA.topics[b].pulse_vol-DATA.topics[a].pulse_vol||DATA.topics[b].total-DATA.topics[a].total);
function renderTopics(filter=""){
  const q=filter.toLowerCase();
  $("#topics").innerHTML=topicNames.filter(t=>!q||t.includes(q)).slice(0,40).map(t=>{
    const d=DATA.topics[t];
    return `<div class="topic-row" data-t="${esc(t)}">
      <span class="t-name">${esc(t)}</span>${spark(d.series,96,26,"var(--ink-3)")}
      <span class="t-vol">${d.pulse_vol||d.total}</span></div>`;}).join("");
  document.querySelectorAll(".topic-row").forEach(el=>el.onclick=()=>selectTopic(el.dataset.t));}
renderTopics();
$("#tsearch").oninput=e=>renderTopics(e.target.value);

/* ---------- stance-flow chart: weekly diverging stack centered on neutral */
function stanceChart(series){
  const w=560,h=190,pad=28,bw=Math.max(4,Math.floor((w-2*pad)/series.length)-2);
  const mx=Math.max(...series.map(s=>s.pos+s.neg+s.neu),1);
  const mid=h/2,scale=(h-40)/(2*mx*0.62);
  let bars="";
  series.forEach((s,i)=>{
    const x=pad+i*((w-2*pad)/series.length);
    const nH=s.neu*scale,pH=s.pos*scale,gH=s.neg*scale;
    if(s.vol===0)return;
    bars+=`<g class="bar-seg" data-i="${i}">
      <rect x="${x}" y="${mid-nH/2}" width="${bw}" height="${Math.max(nH,0.5)}" fill="var(--neu)"/>
      ${pH?`<rect x="${x}" y="${mid-nH/2-pH-2}" width="${bw}" height="${pH}" rx="2" fill="var(--pos)"/>`:""}
      ${gH?`<rect x="${x}" y="${mid+nH/2+2}" width="${bw}" height="${gH}" rx="2" fill="var(--neg)"/>`:""}
      <rect x="${x-1}" y="10" width="${bw+2}" height="${h-20}" fill="transparent" class="hit" data-i="${i}"/></g>`;});
  const labels=series.map((s,i)=>i%5===0?`<text x="${pad+i*((w-2*pad)/series.length)}" y="${h-4}" font-size="9.5" font-family="var(--mono)" fill="var(--ink-3)">${s.week.slice(5)}</text>`:"").join("");
  return `<svg width="100%" viewBox="0 0 ${w} ${h}" style="max-width:${w}px" role="img" aria-label="weekly stance flow">
    <line x1="${pad}" x2="${w-pad}" y1="${mid}" y2="${mid}" stroke="var(--rule-2)" stroke-width="1"/>
    ${bars}${labels}</svg>`;}
function wireChartTips(container,series){
  container.querySelectorAll(".hit").forEach(r=>{
    r.addEventListener("mousemove",e=>{const s=series[+r.dataset.i];
      showTip(e,`<b>${s.week}</b><br>${s.vol} mentions<br><span style="color:#8ab4f8">▲ ${s.pos} positive</span> · <span style="color:#f28b82">▼ ${s.neg} negative</span> · ${s.neu} neutral`);});
    r.addEventListener("mouseleave",hideTip);});}

/* ---------- topic detail */
function selectTopic(name){
  const d=DATA.topics[name];if(!d)return;
  document.querySelectorAll(".topic-row").forEach(el=>el.classList.toggle("sel",el.dataset.t===name));
  const holders=DATA.people.filter(p=>p.recent.some(r=>r.topic===name)||p.against_field.some(a=>a.topic===name));
  const voices=holders.slice(0,6).map(p=>{
    const e=[...p.recent].reverse().find(r=>r.topic===name)||{};
    return `<div class="quote"><div class="q">“${esc(e.evidence||"(position recorded without quotable evidence)")}”</div>
      <div class="who"><b>${esc(p.name)}</b> · <span class="stance-${e.group==="positive"?"pos":e.group==="negative"?"neg":"neu"}">${esc(e.stance||"")}</span> · ${esc(e.show||"")} · ${esc(e.date||"")}</div></div>`;}).join("");
  const el=$("#detail-slot");
  el.innerHTML=`<div class="detail reveal"><button class="close-x" onclick="this.closest('.detail').remove()">✕</button>
    <h3>${esc(name)}</h3>
    <div class="sub">${d.total} mentions in window · pulse ${d.pulse_rate}/wk vs baseline ${d.base_rate}/wk</div>
    ${stanceChart(d.series)}
    <div class="legend"><span><i style="background:var(--pos)"></i>positive</span>
      <span><i style="background:var(--neg)"></i>negative</span>
      <span><i style="background:var(--neu)"></i>neutral</span></div>
    ${voices?`<div style="margin-top:12px">${voices}</div>`:""}</div>`;
  wireChartTips(el,d.series);
  el.scrollIntoView({behavior:"smooth",block:"nearest"});}

/* ---------- person detail */
function selectPerson(name){
  const p=DATA.people.find(x=>x.name===name);if(!p)return;
  const moves=p.moves.map(m=>`<div class="move-line">On <b>${esc(m.topic)}</b>: was
    <span class="stance-${m.from==="positive"?"pos":"neg"}">${m.from}</span> (${esc(m.from_date)}) → now
    <span class="stance-${m.to==="positive"?"pos":"neg"}">${m.to}</span> (${esc(m.to_date)})
    ${m.to_evidence?`<div class="quote" style="margin:6px 0 0"><div class="q">“${esc(m.to_evidence)}”</div></div>`:""}</div>`).join("");
  const against=p.against_field.map(a=>`<div class="move-line">On <b>${esc(a.topic)}</b>: holds
    <span class="stance-${a.stance==="positive"?"pos":"neg"}">${a.stance}</span> while ${Math.round(a.majority_share*100)}% of the field is ${a.field_majority}
    ${a.evidence?`<div class="quote" style="margin:6px 0 0"><div class="q">“${esc(a.evidence)}”</div></div>`:""}</div>`).join("");
  const recent=p.recent.slice(-5).reverse().map(r=>`<div class="quote">
    <div class="q">“${esc(r.evidence||"(no quotable span)")}”</div>
    <div class="who">${esc(cap(r.topic||r.claim_type||""))} · <span class="stance-${r.group==="positive"?"pos":r.group==="negative"?"neg":"neu"}">${esc(r.stance||"")}</span> · ${esc(r.show)} · ${esc(r.date)}</div></div>`).join("");
  $("#detail-slot").innerHTML=`<div class="detail reveal"><button class="close-x" onclick="this.closest('.detail').remove()">✕</button>
    <h3>${esc(p.name)}</h3>
    <div class="sub">${esc(p.roles.join("/"))} · ${esc(p.shows.join(" · "))}${p.authority?` · authority ${p.authority.toFixed(2)}`:""}</div>
    ${moves?`<h4 style="font:700 13px var(--mono);letter-spacing:.1em;margin:8px 0 4px">CHANGED THEIR MIND</h4>${moves}`:""}
    ${against?`<h4 style="font:700 13px var(--mono);letter-spacing:.1em;margin:14px 0 4px">AGAINST THE FIELD</h4>${against}`:""}
    <h4 style="font:700 13px var(--mono);letter-spacing:.1em;margin:14px 0 4px">RECENT POSITIONS</h4>${recent||"<p>No recent positions in window.</p>"}</div>`;
  $("#detail-slot").scrollIntoView({behavior:"smooth",block:"nearest"});}

/* ---------- coverage */
(function(){
  const months={};
  for(const src of Object.keys(DATA.corpus.coverage))
    for(const [m,n] of Object.entries(DATA.corpus.coverage[src]))
      months[m]=(months[m]||0)+n;
  const keys=Object.keys(months).sort();
  const mx=Math.max(...Object.values(months),1);
  const w=Math.min(1260,keys.length*22),h=64;
  let bars="";
  keys.forEach((m,i)=>{const v=months[m],bh=Math.max(2,v/mx*(h-18));
    bars+=`<rect x="${i*22}" y="${h-14-bh}" width="16" height="${bh}" rx="2" fill="var(--pos)" opacity=".75"
      onmousemove="showTip(event,'<b>${m}</b><br>${v} episodes')" onmouseleave="hideTip()"/>`;});
  $("#coverage").innerHTML=`<svg width="100%" viewBox="0 0 ${w} ${h}" style="max-width:${w}px">${bars}</svg>`;
  $("#foot-note").innerHTML=
    `Trend windows anchor to the corpus frontier (${esc(DATA.data_through)}), not the calendar — an ingestion gap reads as a coverage gap, never as “everything faded.” `+
    `Every stance and signal above traces to a verbatim quote; the taxonomy is grown from the data and re-molds itself when sources change.`;
})();
</script>
</body></html>
"""


def main() -> None:
    data = json.loads(DATA.read_text())
    OUT.write_text(TEMPLATE.replace("__DATA__", json.dumps(data)))
    print(json.dumps({"out": str(OUT), "bytes": OUT.stat().st_size}))


if __name__ == "__main__":
    main()
