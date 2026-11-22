# FileMesh-RS

Peer-to-peer file sharing in rooms, built on libp2p. Join a named room, offer a
file, and the people you offer it to get it: chunked, hash-verified, resumable,
with no central server.

## Quick start

```bash
cargo build --release

# Terminal 1
./target/release/filemesh-rs start --name alice --room demo

# Terminal 2 (same LAN: peers find each other automatically via mDNS)
./target/release/filemesh-rs start --name bob --room demo
```

At the `>` prompt:

| Command | What it does |
|---|---|
| `peers` | List room members and how each is connected |
| `send --file <FILE> --to <NAME or PEER_ID>` | Offer a file to one peer |
| `send --file <FILE> --broadcast` | Offer a file to everyone in the room |
| `offers` | List incoming offers waiting for your answer |
| `accept <N>` / `reject <N>` | Answer offer number N |
| `quit` | Exit |

Quote paths with spaces: `send --file "my notes.txt" --to bob`.

Nothing is downloaded until you `accept` (or you started with `--auto-accept`).
Files land in `received_files/` (change with `--download-dir`). Name collisions
become `name (1).ext`; existing files are never overwritten.

## Beyond the local network

mDNS only works on a LAN. For the internet, FileMesh uses a small **relay node**
that you (or anyone) can host. It does three jobs: it relays traffic for peers
behind NAT, it acts as the DHT bootstrap node peers use to find each other, and it
runs an AutoNAT server. Peers then try to upgrade to a direct connection with
hole punching, and fall back to the relay if that fails.

```bash
# 1. On a machine with a public IP (open TCP+UDP port 4001):
filemesh-rs relay --external /ip4/<PUBLIC_IP>/tcp/4001 --external /ip4/<PUBLIC_IP>/udp/4001/quic-v1
#    It prints the --bootstrap address to hand out, e.g.
#    /ip4/<PUBLIC_IP>/tcp/4001/p2p/12D3KooW...

# 2. Anyone, anywhere, with the same room name and that address:
filemesh-rs start --name alice --room demo --bootstrap /ip4/<PUBLIC_IP>/tcp/4001/p2p/12D3KooW...
filemesh-rs start --name bob   --room demo --bootstrap /ip4/<PUBLIC_IP>/tcp/4001/p2p/12D3KooW...
```

Within about ten seconds alice and bob find each other (watch for
`joined the room`). `peers` shows whether each link is `Direct`, `Relayed` or
`Hole-Punched`.

How discovery works: each peer announces the room on the DHT (a provider record
keyed by a hash of the room name) and asks the DHT who else is there every 10 s.
Discovered peers are dialed directly if an address is known, otherwise through
the relay. The DHT is private to FileMesh (its own protocol name), not the public
IPFS network.

For a direct link without a relay, use `--listen` and `--connect`:

```bash
filemesh-rs start --name alice --room demo --listen /ip4/0.0.0.0/tcp/4001
filemesh-rs start --name bob   --room demo --connect /ip4/<ALICE_IP>/tcp/4001
```

Peers listen on both TCP and QUIC by default.

## Options

| Flag | Meaning |
|---|---|
| `--download-dir <DIR>` | Where received files go (default `received_files`) |
| `--auto-accept` | Download incoming files without asking (trusted rooms only) |
| `--key-file <PATH>` | Identity key file (default `~/.filemesh/<name>.key`, created on first run) |
| `--ephemeral` | Throw-away identity for this run |
| `--listen <MULTIADDR>` | Listen address, repeatable (default: TCP and QUIC on any free port) |
| `--connect <MULTIADDR>` | Dial a peer directly at startup (repeatable) |
| `--bootstrap <MULTIADDR>` | Relay/bootstrap node `.../p2p/<PeerId>` for internet discovery (repeatable) |
| `--no-mdns` | Disable local-network discovery |
| `--relay-only` | Reach room members only through the relay: no direct dials, no hole punching (debugging, restrictive networks) |

Your PeerId now survives restarts because the key is saved. Keep the key file
private; anyone holding it can impersonate you.

## How it works

1. **Hash.** The sender splits the file into 1 MiB chunks and hashes each with
   BLAKE3. The list of chunk hashes plus name and size is the *manifest*. The
   hash of the manifest is the file's identity.
2. **Offer.** The sender sends a tiny `Offer` (id, name, size) to the chosen
   peers. No file data moves yet.
3. **Accept.** The receiver fetches the manifest, checks that it matches the
   offered id, then requests chunks (8 in flight at a time).
4. **Verify.** Every chunk is checked against the manifest *before* it is
   written. Bad chunks are re-requested; a run of failures aborts the download.
5. **Resume.** Chunks are written at their final offsets into
   `<download-dir>/.partial/`. After an interruption, accept the offer again
   and only the missing chunks are fetched.

## Layout

| File | Role |
|---|---|
| `src/main.rs` | CLI and interactive prompt |
| `src/libp2p_peers.rs` | Swarm, discovery, offers, downloads, serving |
| `src/protocol.rs` | Wire messages, framing, size limits |
| `src/manifest.rs` | Chunking, hashing, file identity |
| `src/store.rs` | Verified, resumable on-disk chunk storage |
| `src/identity.rs` | Persistent keypair |
| `src/relay.rs` | The `relay` subcommand: circuit relay + DHT bootstrap node |
| `src/room.rs` | Gossipsub room topic |
| `src/util.rs` | Sanitising of names from remote peers |

## Testing

```bash
cargo test              # unit tests (manifest, store, protocol, sanitising, CLI parsing)
./scripts/e2e.sh        # two real peers on loopback, SHA-256 compared
./scripts/relay_e2e.sh  # relay + two peers, every byte through a circuit (~1 min)
```

## Security notes

- A peer's file name is never used as a path: only the final component is kept,
  and bad characters are replaced.
- Frame sizes are capped before anything is allocated.
- A file's id acts as a capability: whoever knows it can fetch the file from the
  sender. Offers go only to the peers you choose.
- Traffic is encrypted (Noise/QUIC) and peers are authenticated by key, including
  when it passes through a relay. The relay can see that two peers talk and how
  much, not what they send.
- There is no room password yet: anyone who knows a room name can find and join
  it, because room membership is discoverable through the DHT.
- A relay you host carries other people's traffic. Use `--max-circuit-bytes` and
  `--max-circuit-secs` to bound it (default: unlimited bytes, 1 hour per circuit).
