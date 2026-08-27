FROM caddy:2-alpine
COPY pif-signal-desk.html /srv/pif-signal-desk.html
COPY pif-signal-desk.html /srv/d/pif-signal-desk.html
COPY pif-signal-desk-funnel.json /srv/pif-signal-desk-funnel.json
COPY index.html /srv/index.html
CMD ["sh", "-c", "caddy file-server --root /srv --listen :${PORT:-8080}"]
