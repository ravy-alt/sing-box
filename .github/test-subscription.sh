#!/usr/bin/env bash
set -euo pipefail

SB_SOURCE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SB_SOURCE_DIR"
SB_TAGS="$(cat release/DEFAULT_BUILD_TAGS_OTHERS)"
SB_LDFLAGS="$(cat release/LDFLAGS)"

go test -race -count=1 -timeout=10m -tags "$SB_TAGS" -ldflags "$SB_LDFLAGS" \
  ./adapter ./adapter/outbound ./common/dependency ./common/group \
  ./common/httpclient ./common/urltest ./common/trafficcontrol ./common/mux \
  ./protocol/group ./protocol/anytls ./protocol/snell ./protocol/ssh \
  ./protocol/hysteria ./protocol/hysteria2 ./protocol/tuic \
  ./transport/v2rayhttp ./transport/v2raygrpc ./transport/v2raygrpclite ./transport/v2rayquic \
  ./protocol/shadowsocks ./protocol/subscription \
  ./experimental/clashapi ./daemon ./dns ./dns/transport ./option ./route

go -C third_party/sing-anytls test -race -count=1 -timeout=5m ./...
