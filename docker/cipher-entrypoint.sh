#!/usr/bin/env bash
set -euo pipefail

: "${OPTIMUSDB_CONTEXT:=cipherkb}"
: "${OPTIMUSDB_API_PORT:=8089}"
: "${OPTIMUSDB_P2P_PORT:=4001}"
: "${OPTIMUSDB_REPO:=/data/cipherkbIpfs}"
: "${OPTIMUSDB_DEVLOGS:=false}"

mkdir -p "$(dirname "${OPTIMUSDB_REPO}")" /var/log/supervisor /var/run

args=(
  "-http=true"
  "-http-port=${OPTIMUSDB_API_PORT}"
  "-ipfs-port=${OPTIMUSDB_P2P_PORT}"
  "-swarmkb=${OPTIMUSDB_CONTEXT}"
  "-repo=${OPTIMUSDB_REPO}"
  "-devlogs=${OPTIMUSDB_DEVLOGS}"
)

# Extra arguments supplied after the image name are forwarded to OptimusDB.
# Example: docker run ... IMAGE -bootstrap=/ip4/.../p2p/...
args+=("$@")

{
  printf '#!/usr/bin/env bash\nset -euo pipefail\nexec /usr/local/bin/optimusdb'
  printf ' %q' "${args[@]}"
  printf '\n'
} > /usr/local/bin/run-optimusdb.sh
chmod 0755 /usr/local/bin/run-optimusdb.sh

exec /usr/bin/supervisord -n -c /etc/supervisor/conf.d/supervisord.conf
