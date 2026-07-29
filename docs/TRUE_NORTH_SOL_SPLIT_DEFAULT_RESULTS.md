# True-North Gold-Author-Class Split-Default Probe

Date: 2026-07-29

Status: terminal partial, acceptance-ineligible; decomposition lane stopped.

## Frozen design

The probe invoked `gpt-5.6-sol` through `codex exec --ephemeral` using Kolby's
Codex subscription. Every receipt records the sol model, high reasoning effort,
and the ephemeral subscription lane. The probe used the exact 19 immutable
split-default semantic packets covering 106 flagged candidates, the unchanged
system prompt, schema, edit contract, validator, scorer, and frozen Task-5
control for unflagged candidates.

The declared ceiling was 16 calls and 450,000 tokens, with 25,000 tokens
reserved per provider envelope and 400,000 reserved for the whole run.

## Operational result

| Measure | Result |
|---|---:|
| Calls | 14 / 16 |
| Tokens | 455,902 / 450,000 |
| Token overage | 5,902 |
| Validated envelopes | 13 / 16 |
| Validated sol candidates | 96 / 106 |
| Validation failures | 1 |

The 25,000-token reservation was below the input tokens alone for most
envelopes. The sequential guard stopped without dispatching calls 15 or 16,
but call 14 itself pushed actual usage 5,902 tokens over the ceiling. One
returned item failed the unchanged rule requiring `edit_reason=none` to adopt
the proposal verbatim.

The run is incomplete and cannot be used as an acceptance result.

## Same-candidate quality

| Comparison | Candidates | Sol | GLM/control |
|---|---:|---:|---:|
| All validated sol candidates vs GLM/Task-5 fallback | 96 | 0.631579 | 0.652632 |
| Direct validated sol vs direct validated GLM | 84 | 0.614458 | 0.638554 |

Of the 36 sol errors on its 96 validated candidates, 21 are under-extractions
and 15 are over-extractions. The gold-author model therefore also fails to
reproduce the gold decomposition judgment and remains directionally
under-split.

A diagnostic partial composition—sol where validated, frozen Task 5 elsewhere—
scores:

- full-fold atomic count: 0.797414;
- aligned faithfulness: 0.755012;
- full-fold matched faithfulness: 0.733522;
- candidate-state macro F1: 0.863223;
- hallucination proxy: 0.193548;
- intrinsic junk escapes: 0;
- relational contamination: 0.

The partial composition is not eligible despite aligned faithfulness passing.
Per Kolby's directive, the decomposition lane is stopped. The measured outcome
is certification with disclosed limitations; no further decomposition design
is proposed.

Cumulative known campaign spend is 264 calls and 2,782,670 tokens. As before,
the token total is a known minimum because historical actor-gold-repair tokens
were never captured in the ledger.

Terminal diagnostic SHA-256:
`07ead4df1381ddab070ae5e278398489f51e3767c9e783ef145cbb7b17d1a3fa`.

