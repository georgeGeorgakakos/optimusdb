#!/usr/bin/env bash
# ============================================================================
# zstaki_portcheck.sh  —  run this ON the Ubuntu server (zstaki)
#
# Two jobs:
#   1) OUTBOUND: test which ports the server can dial OUT to the internet.
#   2) INBOUND : temporarily LISTEN on chosen ports so your Windows laptop
#                can probe them from outside. (This reveals what the cloud
#                security group actually allows IN.)
#
# Usage:
#   chmod +x zstaki_portcheck.sh
#   ./zstaki_portcheck.sh outbound              # just test outbound
#   sudo ./zstaki_portcheck.sh listen 443       # listen on :443 for laptop test
#   sudo ./zstaki_portcheck.sh listen 4001      # listen on :4001, etc.
#   ./zstaki_portcheck.sh all                   # outbound test + listen menu
#
# NOTES:
#   - 'listen' needs sudo for ports < 1024 (e.g. 443).
#   - Do NOT 'listen' on a port already bound by a service (e.g. 80/Traefik,
#     8089/optimusdb). The script warns you if the port is busy.
#   - Ctrl-C to stop a listener and return.
# ============================================================================

set -uo pipefail

# Ports we care about for the OptimusDB mesh plan.
OUT_PORTS=(22 80 443 4001 8080 8089 9000 30011 30012 30013)
PROBE_HOST="portquiz.net"   # answers on ALL ports — perfect for outbound tests

c_green=$'\e[32m'; c_red=$'\e[31m'; c_yellow=$'\e[33m'; c_reset=$'\e[0m'

test_outbound() {
  echo "== OUTBOUND test (server -> ${PROBE_HOST}) =="
  echo "   Shows which ports your cloud security group lets the server dial OUT."
  echo
  printf "%-8s %s\n" "PORT" "RESULT"
  for p in "${OUT_PORTS[@]}"; do
    if timeout 4 bash -c "echo > /dev/tcp/${PROBE_HOST}/${p}" 2>/dev/null; then
      printf "%-8s ${c_green}%s${c_reset}\n" "$p" "OPEN (outbound allowed)"
    else
      printf "%-8s ${c_red}%s${c_reset}\n" "$p" "BLOCKED / filtered"
    fi
  done
  echo
  echo "Interpretation: OPEN outbound ports can reach a relay/peer on the internet."
  echo "You already know :443 out = OPEN, :4001 out = BLOCKED."
  echo
}

port_in_use() {
  # returns 0 if something is already listening on the port
  if command -v ss >/dev/null 2>&1; then
    ss -ltn "( sport = :$1 )" 2>/dev/null | grep -q LISTEN
  else
    netstat -ltn 2>/dev/null | grep -qE "[:.]$1 "
  fi
}

listen_on() {
  local port="$1"
  if [[ -z "$port" ]]; then
    echo "${c_red}No port given.${c_reset} Usage: sudo $0 listen <port>"; return 1
  fi
  if port_in_use "$port"; then
    echo "${c_yellow}WARNING:${c_reset} port ${port} is already in use by a running service."
    echo "Pick a free port, or you'll clash with (e.g.) Traefik/optimusdb."
    return 1
  fi

  echo "== INBOUND listener on :${port} =="
  echo "Server is now LISTENING on 0.0.0.0:${port}."
  echo "From your WINDOWS LAPTOP run:  Test-NetConnection 193.225.250.240 -Port ${port}"
  echo "If the laptop connects and you see a hit below, that port is INBOUND-OPEN."
  echo "Press Ctrl-C to stop."
  echo "-----------------------------------------------------------------------"

  if command -v ncat >/dev/null 2>&1; then
    # ncat keeps accepting connections; prints peer on connect
    ncat -k -l -v "$port"
  elif nc -h 2>&1 | grep -q -- '-k'; then
    nc -k -l -v -p "$port" 2>/dev/null || nc -k -lvp "$port"
  else
    # BusyBox/traditional nc: single-shot, loop it
    echo "(basic nc: will re-arm after each connection)"
    while true; do
      echo ">>> waiting for a connection on :${port} ..."
      nc -l -v -p "$port" 2>/dev/null || nc -lvp "$port"
      echo ">>> connection closed, re-arming ..."
      sleep 0.5
    done
  fi
}

menu_listen() {
  echo "Which port do you want to open a temporary inbound listener on?"
  echo "Common choices: 443 (the one that matters), 4001, 8080, 9000"
  read -rp "Port: " p
  listen_on "$p"
}

case "${1:-all}" in
  outbound) test_outbound ;;
  listen)   listen_on "${2:-}" ;;
  all)      test_outbound; echo; menu_listen ;;
  *)
    echo "Usage: $0 [outbound | listen <port> | all]"
    exit 1
    ;;
esac