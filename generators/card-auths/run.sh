#!/bin/sh
# Start ShadowTraffic with the licence the platform mounts, or idle (healthy, logging why) without
# one: the platform's CI has no licence, and an expired trial must not crash-loop the service.
# The licence is read into the environment line by line, never echoed.
set -eu
licence="${SHADOWTRAFFIC_ENV_FILE:-/run/tenant-secrets/shadowtraffic.env}"

if [ ! -r "$licence" ]; then
  echo "[card-auths] no ShadowTraffic licence at $licence: generator idle (make tenant-secret on the platform)"
  trap 'exit 0' TERM INT
  while :; do sleep 3600 & wait $!; done
fi

while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in ''|'#'*) continue ;; esac
  export "${line%%=*}=${line#*=}"
done < "$licence"
echo "[card-auths] licence loaded (expires ${LICENSE_EXPIRATION:-unknown}); generating to ${KAFKA_BOOTSTRAP:-kafka:9092}"

# SHADOWTRAFFIC_ARGS adds flags, e.g. "--stdout --sample 5" to print records instead (make card-auths-sample).
# shellcheck disable=SC2086
exec java -Djava.security.manager=allow -Dfile.encoding=UTF-8 -XX:MaxRAMPercentage=65.0 \
  -jar /home/shadowtraffic.jar --config /home/card-auths.json ${SHADOWTRAFFIC_ARGS:-}
