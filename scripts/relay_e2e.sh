#!/usr/bin/env bash
# Internet-path smoke test on loopback: relay + two peers that know ONLY the
# relay (no mDNS, no direct dialing, no hole punching), so every byte goes
# through a circuit.
#
#   Scenario 1: unlimited relay -> transfer must succeed, connection = Relayed
#   Scenario 2: relay capped at 128 KiB -> transfer must FAIL (proves the data
#               really flows through the relay, and why the default is unlimited)
#
#   ./scripts/relay_e2e.sh      (takes about a minute)
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-}"
if [ -z "$BIN" ]; then
  for c in "$ROOT/target/release/filemesh-rs" "$ROOT/target/debug/filemesh-rs"; do
    [ -x "$c" ] && BIN="$c" && break
  done
fi
[ -x "${BIN:-/nonexistent}" ] || { echo "Build first: cargo build --release"; exit 1; }

export NO_COLOR=1
BASE_PORT="${PORT:-40301}"
ROOTDIR="$(mktemp -d)"
trap 'pkill -P $$ -f filemesh-rs 2>/dev/null; rm -rf "$ROOTDIR"' EXIT

waitfor() { # file pattern tries(0.5s each)
  for _ in $(seq 1 "$3"); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 0.5; done
  return 1
}

run_scenario() { # name port max_circuit_bytes
  local name=$1 port=$2 limit=$3 d="$ROOTDIR/$1"
  mkdir -p "$d" && cd "$d" || return 1
  head -c 5000000 /dev/urandom > payload.bin
  mkfifo a_in b_in
  local R="/ip4/127.0.0.1/tcp/$port"

  "$BIN" relay --ephemeral --listen "$R" --external "$R" --max-circuit-bytes "$limit" > relay.log 2>&1 &
  local rpid=$!
  waitfor relay.log "Relay node:" 40
  local rid; rid=$(grep "Relay node:" relay.log | awk '{print $3}')
  local flags="--ephemeral --no-mdns --relay-only --bootstrap $R/p2p/$rid"

  "$BIN" start --name alice --room t $flags --download-dir "$d/adl" < a_in > alice.log 2>&1 &
  "$BIN" start --name bob   --room t $flags --download-dir "$d/bdl" < b_in > bob.log 2>&1 &
  exec 3>a_in 4>b_in

  waitfor bob.log "alice joined" 120 || { echo "  discovery via DHT/relay failed"; return 1; }
  sleep 1; echo peers >&4; sleep 1
  echo "send --file $d/payload.bin --to bob" >&3
  waitfor bob.log "Incoming file offer #1" 60 || { echo "  offer never arrived"; return 1; }
  echo "accept 1" >&4
  waitfor bob.log "Received file\|failed" 60
  sleep 1

  RELAYED=0; OK=0
  tr '\r' '\n' < bob.log | grep -q "Connection Type: Relayed" && RELAYED=1
  [ -f bdl/payload.bin ] && cmp -s payload.bin bdl/payload.bin && OK=1
  echo quit >&3; echo quit >&4; exec 3>&- 4>&-
  sleep 1; kill $rpid 2>/dev/null; pkill -P $$ -f filemesh-rs 2>/dev/null
  return 0
}

fail=0

echo "Scenario 1: unlimited relay"
run_scenario unlimited "$BASE_PORT" 0
if [ "$OK" = 1 ] && [ "$RELAYED" = 1 ]; then echo "  PASS: file arrived intact over a relayed connection"
else echo "  FAIL (intact=$OK relayed=$RELAYED)"; fail=1; fi

echo "Scenario 2: relay capped at 128 KiB"
run_scenario capped "$((BASE_PORT + 1))" 131072
if [ "$OK" = 0 ]; then echo "  PASS: transfer was cut off, so the relay is in the data path"
else echo "  FAIL: file arrived despite the cap (relay was bypassed)"; fail=1; fi

exit $fail
