const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function harness(payloads={}) {
  const app={innerHTML:'',scrollTop:0,focus(){}};
  const nodes=new Map();
  const ctx={console,URL,Map,Set,Number,String,Math,Date,Promise,encodeURIComponent,decodeURIComponent,
    location:{hash:'#issue/i'},history:{state:null,replaceState(){}},
    window:{addEventListener(){},setInterval(){},scrollY:0,scrollTo(){}},
    document:{querySelector:()=>app,querySelectorAll:()=>[],addEventListener(){},getElementById:id=>{if(!nodes.has(id))nodes.set(id,{});return nodes.get(id);}},
    fetch:async file=>({ok:true,json:async()=>payloads[file]}),requestAnimationFrame:fn=>fn()};
  vm.createContext(ctx);
  const src=fs.readFileSync(path.join(__dirname,'../scripts/signal_desk_assets/signal-desk.js'),'utf8');
  const testSrc=src.replace('  boot();\n})();', '  globalThis.subject={state, stageCard, recentWindow, researchBucket, evidenceCard, parseRoute, renderIssue, renderVoice, renderEvidence, renderAsk, setIndex(value){INDEX=value}};\n})();');
  assert.notEqual(src,testSrc,'test hook must replace only final boot');
  vm.runInContext(testSrc,ctx);
  ctx.subject.setIndex({issues:[{id:'i',name:'Issue'}],briefing:[],voices:[],aliases:{issues:{},voices:{}},data_through:'2026-08-26'});
  return {ctx,api:ctx.subject,app};
}
function e(id,month,group,person=null){return {id,month,date:month+'-01',show:id,person,evidence:'Evidence '+id,source_url:'https://example.com/'+id,group,stance:group,episode:'Episode'};}

test('zero recent activity cannot be labeled accelerating',()=>{
 const {api}=harness();assert.equal(api.researchBucket({pulse_vol:0,bucket:'accelerating',brief:{decision_grade:true}}),'archive');
});
test('stage conversion does not treat creation of segments as retention',()=>{
 const {api}=harness();const stages=[{label:'Attempted',shows:2,episodes:10,segments:0},{label:'Segmented',shows:2,episodes:8,segments:100}];
 const html=api.stageCard(stages[1],1,stages);assert.match(html,/segments: created at this stage/);assert.match(html,/episodes: 80% retained/);assert.doesNotMatch(html,/segments: 100%/);
});
test('selected month scopes counts, evidence, and stance examples together',async()=>{
 const issue={id:'i',name:'Issue',accepted_evidence:[e('old','2026-07','positive','A'),e('new','2026-08','negative')],uncertain_evidence:[],series:[],related:[]};
 const {api,app}=harness({'./signal-desk-issues.json':{issues:{i:issue}}});
 await api.renderIssue('i','month','2026-08');
 assert.match(app.innerHTML,/<strong>1<\/strong> excerpts in this view/);
 assert.match(app.innerHTML,/Skeptical \/ warning · 1/);assert.match(app.innerHTML,/Supportive · 0/);
 assert.doesNotMatch(app.innerHTML,/Evidence old/);assert.match(app.innerHTML,/Evidence new/);
});
test('candidate issues stay unavailable rather than exposing evidence',async()=>{
 const {api,app}=harness({'./signal-desk-issues.json':{issues:{candidate:{name:'Candidate',accepted_evidence:[e('private','2026-08','positive')],uncertain_evidence:[],series:[]}}}});
 await api.renderIssue('candidate');assert.doesNotMatch(app.innerHTML,/Evidence private/);assert.match(app.innerHTML,/Unavailable/);
});
test('voice evidence retains voice origin and escapes content',()=>{
 const {api}=harness();const item={...e('x','2026-08','neutral'),evidence:'<script>alert(1)</script>',source_url:'javascript:alert(1)'};
 const html=api.evidenceCard(item,'i',{origin:'voice',originId:'p'});
 assert.match(html,/data-tail="voice\/p"/);assert.match(html,/&lt;script&gt;/);assert.doesNotMatch(html,/href="javascript:/);
});
test('malformed hash does not crash route parsing',()=>{
 const {api,ctx}=harness();ctx.location.hash='#issue/%E0%A4%A';assert.equal(api.parseRoute().id,'%E0%A4%A');
});
test('stale async issue response does not overwrite newer navigation',async()=>{
 const {api,app,ctx}=harness();let finish;
 ctx.fetch=()=>new Promise(resolve=>finish=()=>resolve({ok:true,json:async()=>({issues:{i:{name:'Old page',accepted_evidence:[],uncertain_evidence:[],series:[]}}})}));
 const pending=api.renderIssue('i');ctx.location.hash='#voices';app.innerHTML='New navigation';finish();await pending;assert.equal(app.innerHTML,'New navigation');
});

test('all published issue, voice, and reachable evidence views render against the packaged snapshot',async t=>{
 const root=path.join(__dirname,'../site');
 const read=name=>JSON.parse(fs.readFileSync(path.join(root,name),'utf8'));
 const index=read('signal-desk-index.json'),issues=read('signal-desk-issues.json'),voices=read('signal-desk-voices.json');
 const {api,app}=harness({'./signal-desk-issues.json':issues,'./signal-desk-voices.json':voices});api.setIndex(index);
 const ids=new Set([...index.issues,...index.briefing].map(i=>i.id));assert.ok(ids.size>0);
 const evidence=new Set();
 for(const id of ids){assert.ok(issues.issues[id],id);await api.renderIssue(id);assert.match(app.innerHTML,/What is being said/);for(const e of [...issues.issues[id].accepted_evidence,...issues.issues[id].uncertain_evidence])evidence.add(e.id);}
 for(const voice of index.voices){await api.renderVoice(voice.id);assert.match(app.innerHTML,/What the record says/);for(const e of [...voices.voices[voice.id].direct_evidence,...voices.voices[voice.id].mentions])evidence.add(e.id);}
 for(const id of evidence){await api.renderEvidence(id);assert.match(app.innerHTML,/Recorded excerpt|Excerpt in context/,id);assert.doesNotMatch(app.innerHTML,/<h1>That view moved/);}
 t.diagnostic(`${ids.size} issues, ${index.voices.length} voices, ${evidence.size} evidence routes rendered`);
});

test('recent counts use the corpus window even when an issue series ends early',()=>{
 const {api}=harness();api.setIndex({months:['2026-06','2026-07','2026-08']});assert.equal(api.recentWindow({series_preview:[{month:'2025-01'}]}),'2026-06–2026-08');
});
