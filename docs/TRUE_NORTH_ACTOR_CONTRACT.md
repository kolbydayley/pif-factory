# True North `reported_actor` Contract

## Normative definition

> `reported_actor` is the focal actor: the named person, organization, or
> collective whose action, decision, state, or outcome the claim describes,
> when that actor is explicitly named in the evidence and is not the direct
> speaker speaking in their own voice about themselves. It is not restricted to
> sources of reported speech. If no such actor is explicitly named, the field
> is null.

## Annotation rules

- Apply the definition to each existing adjudicated atomic claim independently.
- Use only the exact evidence, claim text, and direct speaker supplied in the
  packet.
- Return the exact concise name span used in the evidence, or `null`.
- Do not change the claim, speaker, evidence, disposition, decomposition, or any
  other gold field.
- Do not infer an unnamed actor from outside knowledge.
- A non-null value that is not a case-insensitive literal span of the evidence
  is mechanically converted to `null`.
- A direct speaker discussing their own action in their own voice is not a
  `reported_actor`.
- When several actors are named, select the focal actor whose action, decision,
  state, or outcome the atomic claim describes.

## Gate semantics

The actor gate is proposed from independently repaired pass-A and pass-B
agreement. Its proposed threshold is:

```text
min(0.95, repaired inter-annotator ceiling - 0.02)
```

Pass C resolves only A/B disagreements into a proposed gold revision. The
canonical development gold and live gate remain unchanged until the explicit
checkpoint is approved.
