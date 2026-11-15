# Production image: builds a self-contained relx release and runs it.
# Used by the `orders` service in docker-compose.yml.

FROM erlang:28.4.2 AS build

# brod -> kafka_protocol -> crc32cer builds its NIF with cmake.
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Dependencies first, so source edits do not invalidate this layer.
COPY rebar.config rebar.lock ./
RUN rebar3 as prod compile --deps_only

COPY config config
COPY include include
COPY priv priv
COPY src src
RUN rebar3 as prod release

# The release bundles ERTS, so the runtime needs only the shared libraries
# ERTS, crypto and the C++ NIF link against. Same Debian release (trixie) as
# the build image.
FROM debian:trixie-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends libssl3t64 libncurses6 libstdc++6 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/order_processing
COPY --from=build /build/_build/prod/rel/order_processing ./

EXPOSE 8080

# foreground keeps the VM as the container's main process, so SIGTERM from
# `docker compose stop` reaches it and the app shuts down in order.
CMD ["bin/order_processing", "foreground"]
