# STAC Browser, built with a GeoRiva config module.
#
# Upstream's image is built from its own repository root and offers the config
# module as a build arg (SB_CONFIG) over a file in that context. A remote git
# build context cannot be given an extra file, so the context has to be ours:
# this clones the pinned tag, drops config.custom.mjs beside it, and then follows
# upstream's Dockerfile step for step. Everything below the `source` stage is a
# transcription of it — keep it that way, and re-read theirs when moving the tag.
#
# Upstream: https://github.com/radiantearth/stac-browser/blob/v5.0.0/Dockerfile

ARG STAC_BROWSER_VERSION=v5.0.0


FROM alpine/git:latest AS source
ARG STAC_BROWSER_VERSION
WORKDIR /src
RUN git clone --depth 1 --branch "${STAC_BROWSER_VERSION}" \
    https://github.com/radiantearth/stac-browser.git .
COPY config.custom.mjs ./config.custom.mjs


FROM node:lts-alpine AS build-step
ARG DYNAMIC_CONFIG=true
ARG historyMode="history"
ENV DYNAMIC_CONFIG="${DYNAMIC_CONFIG}"
ENV SB_historyMode="${historyMode}"
# Read by vite.config.js, which merges this module over the shipped defaults.
# Relative to the build's working directory.
ENV SB_CONFIG="./config.custom.mjs"

WORKDIR /app
# Manifests first, so a change to config.custom.mjs does not reinstall the world.
COPY --from=source /src/package.json /src/package-lock.json ./
RUN npm install
COPY --from=source /src ./
# Uncomments the <script src="./runtime-config.js"> tag, which is what makes the
# entrypoint's runtime config (and SB_pathPrefix) take effect at container start.
RUN if [ "${DYNAMIC_CONFIG}" = "true" ]; then sed -i 's/<!--RC//;s/RC-->//' index.html; fi
RUN npm run build


FROM nginxinc/nginx-unprivileged:1-alpine
ARG pathPrefix="/"

USER root
RUN apk add --no-cache jq pcre-tools

COPY --from=source /src/config.schema.json /etc/nginx/conf.d/config.schema.json
COPY --from=build-step /app/dist /usr/share/nginx/html
COPY --from=source /src/docker/default.conf /etc/nginx/conf.d/default.conf.template
COPY --from=source /src/docker/docker-entrypoint.sh /docker-entrypoint.d/40-stac-browser-entrypoint.sh

# Upstream removes the stock default.conf so a skipped entrypoint fails loudly
# instead of serving nginx's welcome page.
RUN rm -f /etc/nginx/conf.d/default.conf && \
    chown -R nginx:nginx /usr/share/nginx/html && \
    chmod +x /docker-entrypoint.d/40-stac-browser-entrypoint.sh

ENV SB_pathPrefix="${pathPrefix}"

EXPOSE 8080

STOPSIGNAL SIGTERM

USER nginx
