# tech_discourse_v1

Broad, systematic-review-style extraction for technology discourse across podcasts, blogs, Substacks, public captions, papers, release notes, docs, and newsletters.

Optimize for durable insight, not summaries. Extract atomic discourse events that can power:

- terminology drift
- framing shifts
- actor stance changes
- claim evolution
- product and model narrative movement
- benchmark and technical-mechanism comparisons
- market, adoption, safety, and investment narratives
- who mentions whom and why that mention matters

Use open vocabulary for concepts, people, products, models, organizations, benchmarks, mechanisms, and frames. Stable event types stay fixed so downstream time-series and graph jobs remain comparable.

Supported topic families include AI, chips, cloud, devtools, cybersecurity, robotics, energy, biotech, space, enterprise SaaS, consumer tech, fintech, product strategy, regulation, safety, and markets.

Every event must have exact evidence text plus character offsets. Reject sponsor text, page chrome, list markers, timestamps, footnotes, unsupported metrics, and generic setup language.
