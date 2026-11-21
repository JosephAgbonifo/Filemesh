use crate::{
    identity,
    manifest::{FileId, Manifest, DEFAULT_CHUNK_SIZE},
    protocol::{self, ErrorCode, Request, Response, TransferEvent},
    room::Room,
    store::{ChunkError, ChunkStore},
    util::{human_bytes, sanitize_display, sanitize_display_max},
    UserCommand,
};
use anyhow::{Context, Result};
use colored::Colorize;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use libp2p::{
    autonat, dcutr, gossipsub, identify, mdns,
    multiaddr::Protocol,
    noise, ping, relay,
    request_response::{self, OutboundRequestId, ResponseChannel},
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, Swarm, Transport,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::Duration,
};
use tokio::sync::mpsc;

const PROTOCOL_VERSION: &str = "/filemesh/2.0.0";

/// Chunk requests kept in flight per download.
const WINDOW: usize = 8;
/// Consecutive failed/corrupt chunk requests before a download is abandoned.
const MAX_CONSECUTIVE_FAILURES: u32 = 8;
/// Offers we keep waiting for the user's decision; extras are dropped.
const MAX_PENDING_OFFERS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionType {
    Direct,
    Relayed,
    HolePunched,
}

impl std::fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Direct => write!(f, "{}", "Direct".bright_green()),
            ConnectionType::Relayed => write!(f, "{}", "Relayed".bright_yellow()),
            ConnectionType::HolePunched => write!(f, "{}", "Hole-Punched".bright_cyan()),
        }
    }
}

#[derive(NetworkBehaviour)]
pub struct FileMeshBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
    pub relay_client: relay::client::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub autonat: autonat::Behaviour,
    pub file_transfer: protocol::Behaviour,
}

pub struct PeerInfo {
    pub name: Option<String>,
    pub connection_type: ConnectionType,
    pub addrs: Vec<Multiaddr>,
}

pub struct PeerConfig {
    pub peer_name: String,
    pub room_name: String,
    pub download_dir: PathBuf,
    pub auto_accept: bool,
    /// `None` = throw-away identity for this run.
    pub key_file: Option<PathBuf>,
    /// Address to listen on, e.g. `/ip4/0.0.0.0/tcp/0` (any free port).
    pub listen_addr: Multiaddr,
    /// Peers to dial at startup (for connecting beyond the local network).
    pub dial_addrs: Vec<Multiaddr>,
}

/// A file we have hashed and are willing to serve.
struct SharedFile {
    path: PathBuf,
    manifest: Manifest,
}

/// An offer from another peer waiting for the user to accept or reject it.
struct IncomingOffer {
    from: PeerId,
    id: FileId,
    name: String,
    size: u64,
}

/// What an in-flight outbound request was for.
enum Pending {
    Offer { peer: PeerId, name: String },
    Manifest { id: FileId, peer: PeerId, name: String },
    Chunk { id: FileId, index: u32 },
}

enum Step {
    Continue,
    Abort(String),
}

struct Download {
    peer: PeerId,
    name: String,
    store: ChunkStore,
    queue: VecDeque<u32>,
    in_flight: usize,
    failures: u32,
    bar: ProgressBar,
}

impl Download {
    /// Record a failed chunk request: re-queue the chunk, give up after too
    /// many failures in a row.
    fn note_failure(&mut self, index: u32) -> Step {
        self.failures += 1;
        self.queue.push_back(index);
        if self.failures >= MAX_CONSECUTIVE_FAILURES {
            Step::Abort("too many failed or corrupt chunks in a row".to_string())
        } else {
            Step::Continue
        }
    }
}

/// Results of work done on blocking threads, fed back into the event loop.
enum Internal {
    Hashed {
        targets: Vec<PeerId>,
        path: PathBuf,
        result: Result<Manifest>,
    },
    StoreReady {
        id: FileId,
        peer: PeerId,
        name: String,
        result: Result<ChunkStore>,
    },
    Finalized {
        peer: PeerId,
        name: String,
        size: u64,
        result: Result<PathBuf>,
    },
    Served {
        channel: ResponseChannel<Response>,
        response: Response,
        served: Option<(PeerId, FileId, u32)>,
    },
}

pub struct FileMeshPeer {
    swarm: Swarm<FileMeshBehaviour>,
    room: Room,
    peer_name: String,
    connected_peers: HashMap<PeerId, PeerInfo>,
    relay_peers: HashSet<PeerId>,

    download_dir: PathBuf,
    auto_accept: bool,
    listen_addr: Multiaddr,
    dial_addrs: Vec<Multiaddr>,
    internal_tx: mpsc::UnboundedSender<Internal>,

    // Sending side
    shared: HashMap<FileId, SharedFile>,
    served: HashMap<(PeerId, FileId), HashSet<u32>>,

    // Receiving side
    offers: BTreeMap<u32, IncomingOffer>,
    next_offer_no: u32,
    /// Ids currently being fetched (manifest requested .. file finalised).
    active: HashSet<FileId>,
    downloads: HashMap<FileId, Download>,

    pending: HashMap<OutboundRequestId, Pending>,
}

impl FileMeshPeer {
    fn broadcast_join(&mut self) {
        let join_msg = format!("JOIN:{}", self.peer_name);
        if let Err(e) = self.room.broadcast(
            &mut self.swarm.behaviour_mut().gossipsub,
            join_msg.as_bytes(),
        ) {
            eprintln!("{} {}", "Failed to broadcast join:".red(), e);
        }
    }

    fn new(config: PeerConfig, internal_tx: mpsc::UnboundedSender<Internal>) -> Result<Self> {
        let local_key = match &config.key_file {
            Some(path) => identity::load_or_create(path)?,
            None => libp2p::identity::Keypair::generate_ed25519(),
        };
        let local_peer_id = PeerId::from(local_key.public());

        println!(
            "{} {} ({})",
            "Starting peer:".bright_cyan(),
            config.peer_name.bright_white(),
            local_peer_id.to_string().bright_black()
        );

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .context("Invalid gossipsub config")?;

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )
        .map_err(|e| anyhow::anyhow!("Failed to create gossipsub behaviour: {}", e))?;

        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

        let identify = identify::Behaviour::new(identify::Config::new(
            PROTOCOL_VERSION.to_string(),
            local_key.public(),
        ));

        let ping = ping::Behaviour::new(ping::Config::new());

        let (relay_transport, relay_client) = relay::client::new(local_peer_id);

        let dcutr = dcutr::Behaviour::new(local_peer_id);

        let autonat = autonat::Behaviour::new(
            local_peer_id,
            autonat::Config {
                retry_interval: Duration::from_secs(10),
                refresh_interval: Duration::from_secs(30),
                boot_delay: Duration::from_secs(5),
                throttle_server_period: Duration::from_secs(1),
                ..Default::default()
            },
        );

        // Combine relay transport with direct TCP transport so that relay client
        // stays alive and can establish relayed connections.
        let transport = relay_transport
            .or_transport(tcp::tokio::Transport::default())
            .upgrade(libp2p::core::upgrade::Version::V1)
            .authenticate(noise::Config::new(&local_key)?)
            .multiplex(yamux::Config::default())
            .boxed();

        let behaviour = FileMeshBehaviour {
            gossipsub,
            mdns,
            identify,
            ping,
            relay_client,
            dcutr,
            autonat,
            file_transfer: protocol::new_behaviour(),
        };

        let swarm = Swarm::new(
            transport,
            behaviour,
            local_peer_id,
            // Long idle timeout so quiet peers stay in the room between transfers.
            libp2p::swarm::Config::with_tokio_executor()
                .with_idle_connection_timeout(Duration::from_secs(3600)),
        );

        let room = Room::new(config.room_name, local_peer_id);

        Ok(FileMeshPeer {
            swarm,
            room,
            peer_name: config.peer_name,
            connected_peers: HashMap::new(),
            relay_peers: HashSet::new(),
            download_dir: config.download_dir,
            auto_accept: config.auto_accept,
            listen_addr: config.listen_addr,
            dial_addrs: config.dial_addrs,
            internal_tx,
            shared: HashMap::new(),
            served: HashMap::new(),
            offers: BTreeMap::new(),
            next_offer_no: 1,
            active: HashSet::new(),
            downloads: HashMap::new(),
            pending: HashMap::new(),
        })
    }

    fn start(&mut self) -> Result<()> {
        self.swarm.listen_on(self.listen_addr.clone())?;

        self.room.join(&mut self.swarm.behaviour_mut().gossipsub)?;
        self.broadcast_join();
        println!(
            "{} {}",
            "Joined room:".bright_cyan(),
            self.room.name().bright_white()
        );
        println!(
            "{} {}",
            "Saving received files to:".bright_cyan(),
            self.download_dir.display().to_string().bright_white()
        );
        if self.auto_accept {
            println!(
                "{}",
                "Auto-accept is ON: incoming files are downloaded without asking.".yellow()
            );
        }
        for addr in self.dial_addrs.clone() {
            println!("{} {}", "Dialing:".bright_cyan(), addr.to_string().bright_black());
            if let Err(e) = self.swarm.dial(addr) {
                println!("{} {}", "Could not dial:".red(), e);
            }
        }
        println!();
        println!("{}", "Listening for connections...".bright_black());
        println!();

        Ok(())
    }

    pub fn local_peer_id(&self) -> &PeerId {
        self.room.local_peer_id()
    }

    // ---------------------------------------------------------------- peers

    fn label(&self, peer: &PeerId) -> String {
        match self.connected_peers.get(peer).and_then(|i| i.name.clone()) {
            Some(name) => name,
            None => {
                let s = peer.to_string();
                format!("…{}", &s[s.len().saturating_sub(8)..])
            }
        }
    }

    fn room_members(&self) -> Vec<PeerId> {
        self.connected_peers
            .iter()
            .filter(|(_, info)| info.name.is_some())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Resolve `--to` input: a full PeerId, or a room member's name.
    fn resolve_target(&self, input: &str) -> std::result::Result<PeerId, String> {
        if let Ok(id) = input.parse::<PeerId>() {
            return match self.connected_peers.get(&id) {
                None => Err("Peer not connected.".to_string()),
                Some(info) if info.name.is_none() => Err("Peer not in the room.".to_string()),
                Some(_) => Ok(id),
            };
        }

        let matches: Vec<PeerId> = self
            .connected_peers
            .iter()
            .filter(|(_, info)| {
                info.name
                    .as_deref()
                    .map(|n| n.eq_ignore_ascii_case(input))
                    .unwrap_or(false)
            })
            .map(|(id, _)| *id)
            .collect();

        match matches.as_slice() {
            [one] => Ok(*one),
            [] => Err(format!("No peer named '{}' in the room.", input)),
            _ => Err(format!(
                "Several peers are named '{}'. Use the full peer ID (see `peers`).",
                input
            )),
        }
    }

    pub fn list_peers(&self) {
        println!();
        println!(
            "{}",
            "╔════════════════════════════════════════════╗".bright_cyan()
        );
        println!(
            "{}",
            "║           Room Members                     ║".bright_cyan()
        );
        println!(
            "{}",
            "╚════════════════════════════════════════════╝".bright_cyan()
        );
        println!();

        let room_members: Vec<_> = self
            .connected_peers
            .iter()
            .filter(|(_, info)| info.name.is_some())
            .collect();

        if room_members.is_empty() {
            println!("{}", "  No peers in the room yet.".bright_black());
            println!();
            let _ = io::stdout().flush();
            return;
        }

        for (i, (peer_id, info)) in room_members.iter().enumerate() {
            let name = info.name.as_ref().unwrap();

            println!(
                "  {}. {} ({})",
                (i + 1).to_string().bright_white(),
                name.bright_yellow(),
                peer_id.to_string().bright_black()
            );
            println!("     Connection Type: {}", info.connection_type);
            println!(
                "     Addresses: {}",
                info.addrs
                    .first()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "None".to_string())
                    .bright_black()
            );
            println!();
        }

        let _ = io::stdout().flush();
    }

    fn update_connection_type(&mut self, peer_id: PeerId, conn_type: ConnectionType) {
        if let Some(info) = self.connected_peers.get_mut(&peer_id) {
            if info.connection_type != conn_type {
                info.connection_type = conn_type.clone();
                println!(
                    "{} {} {}",
                    "Connection upgraded:".bright_green(),
                    peer_id.to_string().bright_black(),
                    conn_type
                );
            }
        }
    }

    fn on_connection_established(&mut self, peer_id: PeerId, remote: Multiaddr) {
        let relayed = remote.iter().any(|p| matches!(p, Protocol::P2pCircuit));
        let kind = if relayed {
            ConnectionType::Relayed
        } else {
            ConnectionType::Direct
        };

        let info = self.connected_peers.entry(peer_id).or_insert_with(|| PeerInfo {
            name: None,
            connection_type: kind.clone(),
            addrs: Vec::new(),
        });
        if !info.addrs.contains(&remote) {
            info.addrs.push(remote);
        }
        // A direct connection beats a relayed one we had before.
        if !relayed && info.connection_type == ConnectionType::Relayed {
            info.connection_type = ConnectionType::Direct;
        }

        // Tell the new peer who we are.
        self.broadcast_join();
    }

    fn on_join(&mut self, author: PeerId, raw_name: &str) {
        let name = sanitize_display(raw_name);
        // Only record names for peers we are actually connected to.
        if let Some(info) = self.connected_peers.get_mut(&author) {
            if info.name.is_none() {
                println!(
                    "{} {} {}",
                    "→".bright_green(),
                    name.bright_yellow(),
                    "joined the room".bright_black()
                );
            }
            info.name = Some(name);
        }
    }

    fn on_peer_gone(&mut self, peer_id: PeerId) {
        if let Some(info) = self.connected_peers.remove(&peer_id) {
            let name = info.name.as_deref().unwrap_or("Unknown");
            println!(
                "{} {} {}",
                "←".bright_red(),
                name.bright_yellow(),
                "left the room".bright_black()
            );
        }

        self.offers.retain(|_, o| o.from != peer_id);

        let lost: Vec<FileId> = self
            .downloads
            .iter()
            .filter(|(_, d)| d.peer == peer_id)
            .map(|(id, _)| *id)
            .collect();
        for id in lost {
            self.abort_download(id, "connection to the sender was lost");
        }
    }

    // -------------------------------------------------------------- commands

    /// Returns `true` when the peer should shut down.
    fn handle_command(&mut self, cmd: UserCommand) -> bool {
        match cmd {
            UserCommand::ListPeers => {
                println!("{}", "Listing peers...".bright_black());
                self.list_peers();
            }
            UserCommand::SendFile {
                file_path,
                target,
                broadcast,
            } => self.send_file(file_path, target, broadcast),
            UserCommand::Offers => self.list_offers(),
            UserCommand::Accept(n) => match self.offers.remove(&n) {
                Some(o) => self.begin_download(o.from, o.id, o.name),
                None => println!("{} #{}", "No such offer:".red(), n),
            },
            UserCommand::Reject(n) => match self.offers.remove(&n) {
                Some(o) => println!(
                    "{} {}",
                    "Rejected:".bright_yellow(),
                    sanitize_display_max(&o.name, 80)
                ),
                None => println!("{} #{}", "No such offer:".red(), n),
            },
            UserCommand::Quit => {
                println!("{}", "Shutting down peer...".yellow());
                self.room
                    .leave(&mut self.swarm.behaviour_mut().gossipsub)
                    .ok();
                return true;
            }
        }
        false
    }

    fn list_offers(&self) {
        if self.offers.is_empty() {
            println!("{}", "No pending offers.".bright_black());
            return;
        }
        for (n, o) in &self.offers {
            println!(
                "  #{} {} ({}) from {}",
                n.to_string().bright_white(),
                o.name.bright_yellow(),
                human_bytes(o.size),
                self.label(&o.from).bright_cyan()
            );
        }
    }

    fn send_file(&mut self, file_path: String, target: Option<String>, broadcast: bool) {
        let path = PathBuf::from(&file_path);
        match std::fs::metadata(&path) {
            Ok(m) if m.is_file() => {}
            Ok(_) => {
                println!(
                    "{}",
                    "That is not a regular file (folders aren't supported yet).".red()
                );
                return;
            }
            Err(e) => {
                println!("{} {}: {}", "Cannot read".red(), file_path, e);
                return;
            }
        }

        let targets: Vec<PeerId> = if broadcast {
            let members = self.room_members();
            if members.is_empty() {
                println!("{}", "No peers in the room to broadcast to.".red());
                return;
            }
            members
        } else if let Some(t) = target {
            match self.resolve_target(&t) {
                Ok(id) => vec![id],
                Err(msg) => {
                    println!("{}", msg.red());
                    return;
                }
            }
        } else {
            return;
        };

        println!("{} {}", "Hashing:".bright_cyan(), path.display());
        let tx = self.internal_tx.clone();
        tokio::task::spawn_blocking(move || {
            let result = Manifest::from_file(&path, DEFAULT_CHUNK_SIZE);
            let _ = tx.send(Internal::Hashed {
                targets,
                path,
                result,
            });
        });
    }

    // ------------------------------------------------- results from threads

    fn handle_internal(&mut self, event: Internal) {
        match event {
            Internal::Hashed {
                targets,
                path,
                result,
            } => match result {
                Err(e) => println!("{} {:#}", "Could not prepare file:".red(), e),
                Ok(manifest) => {
                    let id = manifest.id();
                    let name = manifest.name.clone();
                    let size = manifest.size;
                    println!(
                        "{} {} ({}, {} chunks, id {})",
                        "Ready to share:".bright_cyan(),
                        name.bright_white(),
                        human_bytes(size),
                        manifest.chunk_count(),
                        hex::encode(&id[..4]).bright_black()
                    );
                    self.shared.insert(id, SharedFile { path, manifest });

                    for peer in targets {
                        if !self.swarm.is_connected(&peer) {
                            println!("{} {}", "No longer connected:".red(), self.label(&peer));
                            continue;
                        }
                        let rid = self.swarm.behaviour_mut().file_transfer.send_request(
                            &peer,
                            Request::Offer {
                                id,
                                name: name.clone(),
                                size,
                            },
                        );
                        self.pending.insert(
                            rid,
                            Pending::Offer {
                                peer,
                                name: name.clone(),
                            },
                        );
                    }
                }
            },

            Internal::StoreReady {
                id,
                peer,
                name,
                result,
            } => match result {
                Err(e) => {
                    self.active.remove(&id);
                    println!("{} {:#}", "Could not prepare download:".red(), e);
                }
                Ok(store) => {
                    let total = store.manifest().size;
                    let bar = ProgressBar::new(total);
                    bar.set_style(
                        ProgressStyle::with_template(
                            "{msg} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta})",
                        )
                        .unwrap()
                        .progress_chars("=> "),
                    );
                    bar.set_message(name.clone());
                    bar.set_position(store.bytes_have());

                    if store.have_count() > 0 {
                        println!(
                            "{} {}/{} chunks already on disk",
                            "Resuming:".bright_cyan(),
                            store.have_count(),
                            store.manifest().chunk_count()
                        );
                    }

                    let queue: VecDeque<u32> = store.missing().into();
                    let complete = store.is_complete();
                    let dl = Download {
                        peer,
                        name,
                        store,
                        queue,
                        in_flight: 0,
                        failures: 0,
                        bar,
                    };
                    if complete {
                        self.finish_download(id, dl);
                    } else {
                        self.downloads.insert(id, dl);
                        self.pump(id);
                    }
                }
            },

            Internal::Finalized {
                peer,
                name,
                size,
                result,
            } => match result {
                Ok(path) => println!(
                    "{} {} {} -> {} ({})",
                    "✓".bright_green(),
                    "Received file:".bright_green(),
                    sanitize_display_max(&name, 80).bright_white(),
                    path.display().to_string().bright_white(),
                    format!("{}, from {}", human_bytes(size), self.label(&peer)).bright_black()
                ),
                Err(e) => println!(
                    "{} {} {:#}",
                    "✗".red(),
                    "Could not finish file:".red(),
                    e
                ),
            },

            Internal::Served {
                channel,
                response,
                served,
            } => {
                let sent = self
                    .swarm
                    .behaviour_mut()
                    .file_transfer
                    .send_response(channel, response)
                    .is_ok();
                if sent {
                    if let Some((peer, id, index)) = served {
                        self.record_served(peer, id, index);
                    }
                }
            }
        }
    }

    fn record_served(&mut self, peer: PeerId, id: FileId, index: u32) {
        let Some(shared) = self.shared.get(&id) else {
            return;
        };
        let total = shared.manifest.chunk_count() as usize;
        let name = shared.manifest.name.clone();

        let set = self.served.entry((peer, id)).or_default();
        set.insert(index);
        if set.len() == total {
            self.served.remove(&(peer, id));
            self.print_sent(&name, &peer);
        }
    }

    fn print_sent(&self, name: &str, peer: &PeerId) {
        println!(
            "{} {} {} {}",
            "✓".bright_green(),
            "Sent file:".bright_green(),
            sanitize_display_max(name, 80).bright_white(),
            format!("to {}", self.label(peer)).bright_black()
        );
    }

    // ------------------------------------------------------------ receiving

    fn on_offer(&mut self, from: PeerId, id: FileId, name: String, size: u64) {
        if self.auto_accept {
            println!(
                "{} {} ({}) from {}",
                "Auto-accepting:".yellow(),
                name.bright_white(),
                human_bytes(size),
                self.label(&from).bright_cyan()
            );
            self.begin_download(from, id, name);
            return;
        }

        if self.offers.values().any(|o| o.from == from && o.id == id) {
            return; // same offer again
        }
        if self.offers.len() >= MAX_PENDING_OFFERS {
            println!(
                "{} {}",
                "Dropping offer, too many pending offers from:".yellow(),
                self.label(&from)
            );
            return;
        }

        let n = self.next_offer_no;
        self.next_offer_no += 1;
        println!(
            "\n{} #{}: {} ({}) from {}",
            "Incoming file offer".bright_yellow(),
            n,
            name.bright_white(),
            human_bytes(size),
            self.label(&from).bright_cyan()
        );
        println!(
            "  Type {} or {}",
            format!("accept {}", n).bright_green(),
            format!("reject {}", n).bright_red()
        );
        self.offers.insert(
            n,
            IncomingOffer {
                from,
                id,
                name,
                size,
            },
        );
    }

    fn begin_download(&mut self, peer: PeerId, id: FileId, name: String) {
        if self.active.contains(&id) {
            println!("{}", "That file is already being downloaded.".yellow());
            return;
        }
        if !self.swarm.is_connected(&peer) {
            println!("{} {}", "No longer connected:".red(), self.label(&peer));
            return;
        }

        self.active.insert(id);
        println!(
            "{} {} {}",
            "Requesting".bright_cyan(),
            sanitize_display_max(&name, 80).bright_white(),
            format!("from {}", self.label(&peer)).bright_black()
        );
        let rid = self
            .swarm
            .behaviour_mut()
            .file_transfer
            .send_request(&peer, Request::GetManifest { id });
        self.pending
            .insert(rid, Pending::Manifest { id, peer, name });
    }

    fn on_manifest(&mut self, id: FileId, peer: PeerId, name: String, manifest: Manifest) {
        if let Err(e) = manifest.validate() {
            self.active.remove(&id);
            println!("{} {:#}", "Rejected a bad manifest:".red(), e);
            return;
        }
        if manifest.id() != id {
            self.active.remove(&id);
            println!(
                "{}",
                "Rejected manifest: it does not match the file that was offered.".red()
            );
            return;
        }

        // Opening may scan a large partial file, so keep it off the event loop.
        let dest = self.download_dir.clone();
        let tx = self.internal_tx.clone();
        tokio::task::spawn_blocking(move || {
            let result = ChunkStore::open(&dest, manifest);
            let _ = tx.send(Internal::StoreReady {
                id,
                peer,
                name,
                result,
            });
        });
    }

    /// Keep up to [`WINDOW`] chunk requests in flight.
    fn pump(&mut self, id: FileId) {
        let Some(dl) = self.downloads.get_mut(&id) else {
            return;
        };
        while dl.in_flight < WINDOW {
            let Some(index) = dl.queue.pop_front() else {
                break;
            };
            let rid = self
                .swarm
                .behaviour_mut()
                .file_transfer
                .send_request(&dl.peer, Request::GetChunk { id, index });
            self.pending.insert(rid, Pending::Chunk { id, index });
            dl.in_flight += 1;
        }
    }

    fn on_chunk(&mut self, id: FileId, expected: u32, got: u32, data: Vec<u8>) {
        let step = {
            let Some(dl) = self.downloads.get_mut(&id) else {
                return;
            };
            dl.in_flight = dl.in_flight.saturating_sub(1);

            if got != expected {
                dl.note_failure(expected)
            } else {
                match dl.store.write_chunk(expected, &data) {
                    Ok(_) => {
                        dl.failures = 0;
                        dl.bar.inc(data.len() as u64);
                        Step::Continue
                    }
                    Err(e) if e.is_bad_data() => dl.note_failure(expected),
                    Err(ChunkError::Io(e)) => Step::Abort(format!("disk error: {}", e)),
                    Err(e) => Step::Abort(e.to_string()),
                }
            }
        };

        match step {
            Step::Abort(reason) => self.abort_download(id, &reason),
            Step::Continue => {
                let complete = self
                    .downloads
                    .get(&id)
                    .map(|d| d.store.is_complete())
                    .unwrap_or(false);
                if complete {
                    if let Some(dl) = self.downloads.remove(&id) {
                        self.finish_download(id, dl);
                    }
                } else {
                    self.pump(id);
                }
            }
        }
    }

    fn finish_download(&mut self, id: FileId, dl: Download) {
        dl.bar.finish_and_clear();
        self.active.remove(&id);

        let Download {
            peer, name, store, ..
        } = dl;
        let size = store.manifest().size;
        let tx = self.internal_tx.clone();
        tokio::task::spawn_blocking(move || {
            let result = store.finalize();
            let _ = tx.send(Internal::Finalized {
                peer,
                name,
                size,
                result,
            });
        });
    }

    fn abort_download(&mut self, id: FileId, reason: &str) {
        self.active.remove(&id);
        if let Some(dl) = self.downloads.remove(&id) {
            dl.bar.abandon();
            println!(
                "{} {} {}",
                "✗".red(),
                format!(
                    "Download of {} failed:",
                    sanitize_display_max(&dl.name, 80)
                )
                .red(),
                reason
            );
            println!(
                "{}",
                "  Partial data was kept. Ask the sender to send it again to resume."
                    .bright_black()
            );
        }
    }

    // -------------------------------------------------------------- serving

    fn handle_transfer_event(&mut self, event: TransferEvent) {
        use request_response::{Event, Message};
        match event {
            Event::Message { peer, message } => match message {
                Message::Request {
                    request, channel, ..
                } => self.handle_request(peer, request, channel),
                Message::Response {
                    request_id,
                    response,
                } => self.handle_response(request_id, response),
            },
            Event::OutboundFailure {
                request_id, error, ..
            } => self.handle_outbound_failure(request_id, error.to_string()),
            Event::InboundFailure { .. } | Event::ResponseSent { .. } => {}
        }
    }

    fn respond(&mut self, channel: ResponseChannel<Response>, response: Response) {
        let _ = self
            .swarm
            .behaviour_mut()
            .file_transfer
            .send_response(channel, response);
    }

    fn handle_request(
        &mut self,
        peer: PeerId,
        request: Request,
        channel: ResponseChannel<Response>,
    ) {
        match request {
            Request::Offer { id, name, size } => {
                self.respond(channel, Response::OfferReceived);
                let name = sanitize_display_max(&name, 80);
                self.on_offer(peer, id, name, size);
            }

            Request::GetManifest { id } => match self.shared.get(&id) {
                Some(shared) => {
                    let manifest = shared.manifest.clone();
                    let empty = manifest.chunk_count() == 0;
                    let name = manifest.name.clone();
                    self.respond(channel, Response::Manifest(manifest));
                    if empty {
                        // Nothing to transfer in chunks; the manifest is the file.
                        self.print_sent(&name, &peer);
                    }
                }
                None => self.respond(channel, Response::Error(ErrorCode::NotFound)),
            },

            Request::GetChunk { id, index } => self.serve_chunk(peer, id, index, channel),
        }
    }

    fn serve_chunk(
        &mut self,
        peer: PeerId,
        id: FileId,
        index: u32,
        channel: ResponseChannel<Response>,
    ) {
        let Some(shared) = self.shared.get(&id) else {
            self.respond(channel, Response::Error(ErrorCode::NotFound));
            return;
        };
        let Some(len) = shared.manifest.chunk_len(index) else {
            self.respond(channel, Response::Error(ErrorCode::OutOfRange));
            return;
        };

        let path = shared.path.clone();
        let offset = shared.manifest.chunk_offset(index);
        let expected_hash = shared.manifest.chunk_hashes[index as usize];
        let tx = self.internal_tx.clone();

        tokio::task::spawn_blocking(move || {
            let read = (|| -> io::Result<Vec<u8>> {
                let mut file = File::open(&path)?;
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; len];
                file.read_exact(&mut buf)?;
                Ok(buf)
            })();

            let (response, served) = match read {
                Ok(buf) if blake3::hash(&buf).as_bytes() == &expected_hash => (
                    Response::Chunk { index, data: buf },
                    Some((peer, id, index)),
                ),
                Ok(_) => {
                    eprintln!(
                        "{} {}",
                        "Shared file changed on disk after hashing:".red(),
                        path.display()
                    );
                    (Response::Error(ErrorCode::Internal), None)
                }
                Err(e) => {
                    eprintln!("{} {}: {}", "Could not read".red(), path.display(), e);
                    (Response::Error(ErrorCode::Internal), None)
                }
            };

            let _ = tx.send(Internal::Served {
                channel,
                response,
                served,
            });
        });
    }

    // ------------------------------------------------------------ responses

    fn handle_response(&mut self, request_id: OutboundRequestId, response: Response) {
        let Some(pending) = self.pending.remove(&request_id) else {
            return;
        };

        match (pending, response) {
            (Pending::Offer { peer, name }, Response::OfferReceived) => println!(
                "{}",
                format!(
                    "Offered {} to {}, waiting for them to accept.",
                    sanitize_display_max(&name, 80),
                    self.label(&peer)
                )
                .bright_black()
            ),
            (Pending::Offer { peer, .. }, Response::Error(code)) => println!(
                "{} {}: {}",
                "Offer not accepted by".red(),
                self.label(&peer),
                code
            ),
            (Pending::Offer { .. }, _) => {}

            (Pending::Manifest { id, peer, name }, Response::Manifest(m)) => {
                self.on_manifest(id, peer, name, m)
            }
            (Pending::Manifest { id, .. }, Response::Error(code)) => {
                self.active.remove(&id);
                println!("{} {}", "Could not fetch file info:".red(), code);
            }
            (Pending::Manifest { id, .. }, _) => {
                self.active.remove(&id);
                println!("{}", "Peer sent an unexpected reply.".red());
            }

            (Pending::Chunk { id, index }, Response::Chunk { index: got, data }) => {
                self.on_chunk(id, index, got, data)
            }
            (Pending::Chunk { id, .. }, Response::Error(code)) => {
                self.abort_download(id, &code.to_string())
            }
            (Pending::Chunk { id, .. }, _) => {
                self.abort_download(id, "peer sent an unexpected reply")
            }
        }
    }

    fn handle_outbound_failure(&mut self, request_id: OutboundRequestId, error: String) {
        let Some(pending) = self.pending.remove(&request_id) else {
            return;
        };

        match pending {
            Pending::Offer { peer, name } => println!(
                "{} {} {}: {}",
                "Could not deliver offer for".red(),
                sanitize_display_max(&name, 80),
                format!("to {}", self.label(&peer)),
                error
            ),
            Pending::Manifest { id, .. } => {
                self.active.remove(&id);
                println!("{} {}", "Could not fetch file info:".red(), error);
            }
            Pending::Chunk { id, index } => {
                let step = match self.downloads.get_mut(&id) {
                    Some(dl) => {
                        dl.in_flight = dl.in_flight.saturating_sub(1);
                        dl.note_failure(index)
                    }
                    None => return, // already aborted
                };
                match step {
                    Step::Abort(reason) => {
                        self.abort_download(id, &format!("{} (last error: {})", reason, error))
                    }
                    Step::Continue => self.pump(id),
                }
            }
        }
    }

    // --------------------------------------------------------- swarm events

    fn handle_swarm_event(&mut self, event: SwarmEvent<FileMeshBehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(FileMeshBehaviourEvent::Gossipsub(
                gossipsub::Event::Message {
                    propagation_source,
                    message,
                    ..
                },
            )) => {
                if message.topic != self.room.topic().hash() {
                    return;
                }
                // `source` is the original author; `propagation_source` is only
                // whoever forwarded it to us.
                let author = message.source.unwrap_or(propagation_source);
                if author == *self.local_peer_id() {
                    return;
                }
                if let Ok(text) = std::str::from_utf8(&message.data) {
                    if let Some(name) = text.strip_prefix("JOIN:") {
                        self.on_join(author, name);
                    }
                }
            }

            // A peer just subscribed to our room: tell them who we are.
            SwarmEvent::Behaviour(FileMeshBehaviourEvent::Gossipsub(
                gossipsub::Event::Subscribed { topic, .. },
            )) => {
                if topic == self.room.topic().hash() {
                    self.broadcast_join();
                }
            }

            SwarmEvent::Behaviour(FileMeshBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer_id, addr) in list {
                    if peer_id != *self.local_peer_id() && !self.swarm.is_connected(&peer_id) {
                        self.swarm.dial(addr).ok();
                    }
                }
            }

            SwarmEvent::Behaviour(FileMeshBehaviourEvent::Identify(
                identify::Event::Received { peer_id, info },
            )) => {
                if let Some(entry) = self.connected_peers.get_mut(&peer_id) {
                    for addr in &info.listen_addrs {
                        if !entry.addrs.contains(addr) {
                            entry.addrs.push(addr.clone());
                        }
                    }
                }

                if info
                    .protocols
                    .iter()
                    .any(|p| p.as_ref() == "/libp2p/circuit/relay/0.2.0/hop")
                {
                    self.relay_peers.insert(peer_id);
                    println!(
                        "{} {}",
                        "Discovered relay peer:".bright_cyan(),
                        peer_id.to_string().bright_black()
                    );
                }
            }

            SwarmEvent::Behaviour(FileMeshBehaviourEvent::Dcutr(dcutr::Event {
                remote_peer_id,
                result,
            })) => match result {
                Ok(_) => self.update_connection_type(remote_peer_id, ConnectionType::HolePunched),
                Err(e) => println!(
                    "{} {}",
                    "Hole punching failed:".bright_black(),
                    e.to_string().bright_black()
                ),
            },

            SwarmEvent::Behaviour(FileMeshBehaviourEvent::Autonat(
                autonat::Event::StatusChanged { old, new },
            )) => {
                println!(
                    "{} {:?} → {:?}",
                    "NAT status changed:".bright_cyan(),
                    old,
                    new
                );
            }

            SwarmEvent::Behaviour(FileMeshBehaviourEvent::FileTransfer(event)) => {
                self.handle_transfer_event(event)
            }

            SwarmEvent::NewListenAddr { address, .. } => {
                println!(
                    "{} {}",
                    "Listening on:".bright_cyan(),
                    address.to_string().bright_black()
                );
            }

            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => self.on_connection_established(peer_id, endpoint.get_remote_address().clone()),

            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                // Only treat the peer as gone once its last connection closes.
                if num_established == 0 {
                    self.on_peer_gone(peer_id);
                }
            }

            _ => {}
        }
    }
}

pub async fn run_peer(
    config: PeerConfig,
    mut cmd_rx: mpsc::UnboundedReceiver<UserCommand>,
) -> Result<()> {
    let (internal_tx, mut internal_rx) = mpsc::unbounded_channel();
    let mut peer = FileMeshPeer::new(config, internal_tx)?;
    peer.start()?;

    loop {
        // Prioritise user commands to keep the UI responsive.
        tokio::select! {
            biased;
            Some(cmd) = cmd_rx.recv() => {
                if peer.handle_command(cmd) {
                    break;
                }
            }
            Some(event) = internal_rx.recv() => peer.handle_internal(event),
            event = peer.swarm.select_next_some() => peer.handle_swarm_event(event),
        }
    }

    Ok(())
}
