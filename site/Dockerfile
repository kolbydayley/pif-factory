FROM caddy:2-alpine
COPY pif-signal-desk.html /srv/pif-signal-desk.html
COPY pif-signal-desk.html /srv/d/pif-signal-desk.html
COPY signal-desk.css /srv/signal-desk.css
COPY signal-desk.js /srv/signal-desk.js
COPY signal-desk-index.json /srv/signal-desk-index.json
COPY signal-desk-issues.json /srv/signal-desk-issues.json
COPY signal-desk-voices.json /srv/signal-desk-voices.json
COPY signal-desk-network.json /srv/signal-desk-network.json
COPY pif-signal-desk-funnel.json /srv/pif-signal-desk-funnel.json
COPY index.html /srv/index.html
CMD ["sh", "-c", "caddy file-server --root /srv --listen :${PORT:-8080}"]
