#!/usr/bin/env bash
# End-to-end smoke test: two real peers on loopback, one file transferred and
# compared by SHA-256, plus a rejected offer.
#
#   ./scripts/e2e.sh            # uses target/release (or target/debug) binary
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-}"
if [ -z "$BIN" ]; then
  for candidate in "$ROOT/target/release/filemesh-rs" "$ROOT/target/debug/filemesh-rs"; do
    [ -x "$candidate" ] && BIN="$candidate" && break
  done
fi
[ -x "${BIN:-/nonexistent}" ] || { echo "Build first: cargo build --release"; exit 1; }

export NO_COLOR=1
WORK="$(mktemp -d)"
PORT="${PORT:-40211}"
cleanup() { exec 3>&- 4>&- 2>/dev/null || true; pkill -P $$ -f filemesh-rs 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

head -c 3500000 /dev/urandom > "$WORK/payload one.bin"   # not a chunk multiple
echo "second file" > "$WORK/second.txt"
mkfifo "$WORK/a_in" "$WORK/b_in"

"$BIN" start --name alice --room e2e --ephemeral \
  --listen "/ip4/127.0.0.1/tcp/$PORT" --download-dir "$WORK/alice_dl" \
  < "$WORK/a_in" > "$WORK/alice.log" 2>&1 &
sleep 1
"$BIN" start --name bob --room e2e --ephemeral \
  --connect "/ip4/127.0.0.1/tcp/$PORT" --download-dir "$WORK/bob_dl" \
  < "$WORK/b_in" > "$WORK/bob.log" 2>&1 &
exec 3>"$WORK/a_in" 4>"$WORK/b_in"

sleep 6
echo "send --file \"$WORK/payload one.bin\" --to bob" >&3
sleep 4
echo "accept 1" >&4
sleep 6
echo "send --file \"$WORK/second.txt\" --to bob" >&3
sleep 3
echo "reject 2" >&4
sleep 2
echo quit >&3; echo quit >&4
sleep 1

fail() { echo "FAIL: $1"; echo "--- alice"; cat "$WORK/alice.log"; echo "--- bob"; cat "$WORK/bob.log"; exit 1; }

[ -f "$WORK/bob_dl/payload one.bin" ] || fail "file was not received"
want="$(sha256sum "$WORK/payload one.bin" | cut -d' ' -f1)"
got="$(sha256sum "$WORK/bob_dl/payload one.bin" | cut -d' ' -f1)"
[ "$want" = "$got" ] || fail "checksum mismatch"
[ ! -e "$WORK/bob_dl/second.txt" ] || fail "rejected file was saved anyway"
echo "PASS: transfer verified ($want), rejected offer was not saved"
